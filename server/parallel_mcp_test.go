package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Autumn-27/artex/agent"
	"github.com/Autumn-27/artex/db"
	"github.com/Autumn-27/norma/llm"
	"github.com/Autumn-27/norma/permission"
	actool "github.com/Autumn-27/norma/tool"
)

type parallelTransport func(*http.Request) (*http.Response, error)

func (f parallelTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// This is a scripted model, so the test proves runtime dispatch and result
// consumption without requiring an external model or credentials.
type parallelScript struct {
	complete func(llm.CompletionRequest) llm.Message
}

func (p parallelScript) Complete(_ context.Context, r llm.CompletionRequest) (llm.Message, string, llm.Usage, error) {
	m := p.complete(r)
	return m, "end_turn", llm.Usage{}, nil
}
func (p parallelScript) Stream(context.Context, llm.CompletionRequest) iter.Seq2[llm.StreamEvent, error] {
	return func(yield func(llm.StreamEvent, error) bool) {
		yield(llm.StreamEvent{}, errors.New("expected non-streaming"))
	}
}

func TestParallelMCPPresetRuntime(t *testing.T) {
	dsn, _, err := db.DSN()
	if err != nil {
		t.Skip("test postgres not configured")
	}
	pg, err := db.Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pg.Close() })
	all, err := pg.ListMCP()
	if err != nil {
		t.Fatal(err)
	}
	var preset *db.MCPServer
	for _, m := range all {
		if m.Name == "parallel-search" {
			preset = m
		}
	}
	if preset == nil {
		t.Fatal("startup did not seed parallel-search")
	}
	original := *preset
	t.Cleanup(func() { _, _ = pg.SaveMCP(&original) })
	a, err := pg.CreateAgent(fmt.Sprintf("parallel_test_%d", time.Now().UnixNano()), "Parallel fixture", "Local MCP runtime test")
	if err != nil || a == nil {
		t.Fatalf("fixture agent: %v", err)
	}
	t.Cleanup(func() { _ = pg.DeleteAgent(a.Key) })
	oldAugment, oldResolve := agent.ToolAugment, agent.ToolResolve
	t.Cleanup(func() { agent.ToolAugment, agent.ToolResolve = oldAugment, oldResolve })
	agent.ToolResolve = nil
	wireAgentAugment(pg, t.TempDir(), nil)

	var mu sync.Mutex
	var requests []string
	fixture := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
			Params struct {
				Name      string         `json:"name"`
				Arguments map[string]any `json:"arguments"`
			} `json:"params"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		requests = append(requests, req.Method+":"+req.Params.Name)
		mu.Unlock()
		if r.URL.Path != "/mcp" || r.Header.Get("User-Agent") != "ARTEX (+https://github.com/trhhubsd/ARTEX)" || r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected request: %s %v", r.URL, r.Header)
		}
		w.Header().Set("Content-Type", "application/json")
		var result any
		switch req.Method {
		case "initialize":
			result = map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "serverInfo": map[string]any{"name": "fixture", "version": "1"}}
		case "notifications/initialized":
			w.WriteHeader(http.StatusAccepted)
			return
		case "tools/list":
			result = map[string]any{"tools": []any{
				map[string]any{"name": "web_search", "description": "Search public documentation", "inputSchema": map[string]any{"type": "object"}},
				map[string]any{"name": "web_fetch", "description": "Read public documentation", "inputSchema": map[string]any{"type": "object"}},
			}}
		case "tools/call":
			if req.Params.Arguments["objective"] == "wait" {
				<-r.Context().Done()
				return
			}
			if req.Params.Arguments["objective"] == "fail" {
				w.WriteHeader(http.StatusServiceUnavailable)
				fmt.Fprint(w, "fixture unavailable")
				return
			}
			if req.Params.Name == "web_search" {
				if req.Params.Arguments["objective"] != "Go language reference" || req.Params.Arguments["search_queries"] == nil {
					t.Errorf("search arguments: %v", req.Params.Arguments)
				}
			} else if req.Params.Name == "web_fetch" {
				b, _ := json.Marshal(req.Params.Arguments["urls"])
				if string(b) != `["https://go.dev/ref/spec"]` {
					t.Errorf("fetch URLs: %s", b)
				}
			} else {
				t.Errorf("unexpected tool: %s", req.Params.Name)
			}
			result = map[string]any{"content": []any{map[string]any{"type": "text", "text": "Go language reference https://go.dev/ref/spec"}}}
		default:
			t.Errorf("unexpected method: %s", req.Method)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result})
	}))
	defer fixture.Close()
	// Redirect only the exact production destination to a local fixture. Verify
	// URL and headers before transport, rather than replacing the preset itself.
	local, _ := url.Parse(fixture.URL)
	oldTransport := http.DefaultTransport
	http.DefaultTransport = parallelTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.String() != "https://search.parallel.ai/mcp" {
			return nil, fmt.Errorf("unexpected destination: %s", r.URL)
		}
		clone := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = local.Scheme, local.Host
		clone.URL, clone.Host = &u, local.Host
		return oldTransport.RoundTrip(clone)
	})
	t.Cleanup(func() { http.DefaultTransport = oldTransport })

	assemble := func() ([]actool.CoreTool, agent.DeferredInfo, func()) {
		return agent.AugmentTools(t.Context(), a.Key, nil)
	}
	// Disabled and invisible states must not even connect to the provider.
	preset.Enabled = false
	if _, err := pg.SaveMCP(preset); err != nil {
		t.Fatal(err)
	}
	if err := pg.ToggleVisibility(a.ID, "mcp", preset.ID, true); err != nil {
		t.Fatal(err)
	}
	tools, _, closeTools := assemble()
	closeTools()
	if len(tools) != 0 {
		t.Fatal("disabled MCP surfaced tools")
	}
	preset.Enabled = true
	if _, err := pg.SaveMCP(preset); err != nil {
		t.Fatal(err)
	}
	if err := pg.ToggleVisibility(a.ID, "mcp", preset.ID, false); err != nil {
		t.Fatal(err)
	}
	tools, _, closeTools = assemble()
	closeTools()
	if len(tools) != 0 || len(requests) != 0 {
		t.Fatal("invisible MCP connected")
	}
	if err := pg.ToggleVisibility(a.ID, "mcp", preset.ID, true); err != nil {
		t.Fatal(err)
	}

	tools, def, closeTools := assemble()
	defer closeTools()
	if len(tools) != 2 || len(def.Deferred) != 2 || !def.Unlock.Has("mcp__parallel-search__web_search") {
		t.Fatalf("bad tool assembly: %+v", def)
	}
	search := tools[0]
	if search.CheckPermissions(t.Context(), nil, permission.Context{}).Behavior != permission.Ask {
		t.Fatal("MCP approval semantics changed")
	}
	if got := permission.Evaluate(t.Context(), permission.Request{ToolName: search.Name(), ToolDecision: search.CheckPermissions(t.Context(), nil, permission.Context{}), Ctx: permission.Context{Mode: permission.ModeBypass, Disallowed: []string{search.Name()}}}); got.Behavior != permission.Deny {
		t.Fatal("explicit permission denial lost")
	}
	for _, objective := range []string{"fail", "wait"} {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		in, _ := json.Marshal(map[string]any{"objective": objective})
		res, err := search.Call(ctx, in, nil)
		cancel()
		if err != nil || !res.IsError || (objective == "fail" && !strings.Contains(res.Flatten(), "503")) {
			t.Fatalf("%s: result=%+v err=%v", objective, res, err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	res, err := search.Call(ctx, json.RawMessage(`{"objective":"wait"}`), nil)
	if err != nil || !res.IsError || !strings.Contains(res.Flatten(), "context canceled") {
		t.Fatalf("cancellation: %+v %v", res, err)
	}

	step := 0
	p := parallelScript{complete: func(r llm.CompletionRequest) llm.Message {
		step++
		if step > 1 {
			data, _ := json.Marshal(r.Messages)
			want := "Go language reference https://go.dev/ref/spec"
			if step == 2 {
				want = "mcp__parallel-search__web_search"
			}
			if !strings.Contains(string(data), want) {
				t.Errorf("step %d did not consume previous result: %s", step, data)
			}
		}
		var name, input string
		switch step {
		case 1:
			name, input = "SearchExtraTools", `{"query":"parallel-search"}`
		case 2:
			name, input = "ExecuteExtraTool", `{"tool_name":"mcp__parallel-search__web_search","params":{"objective":"Go language reference","search_queries":["Go language reference specification"]}}`
		case 3:
			name, input = "ExecuteExtraTool", `{"tool_name":"mcp__parallel-search__web_fetch","params":{"urls":["https://go.dev/ref/spec"]}}`
		default:
			return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{llm.TextBlock("Reference checked.")}}
		}
		return llm.Message{Role: llm.RoleAssistant, Content: []llm.ContentBlock{{Type: llm.BlockToolUse, ID: fmt.Sprint(step), Name: name, Input: json.RawMessage(input)}}}
	}}
	chat := agent.NewChatAgent(p, "scripted-test", t.TempDir(), nil, 100000)
	chat.SetNonStreaming(func() bool { return true })
	chat.SetWebSearch(agent.WebSearchOpts{Enabled: false})
	text, err := chat.Chat(t.Context(), a.Key, "parallel-preset-test", "Read the Go language reference.", 6, time.Minute, false, nil)
	if err != nil || text != "Reference checked." || step != 4 {
		t.Fatalf("chat: text=%q steps=%d err=%v", text, step, err)
	}
	mu.Lock()
	defer mu.Unlock()
	joined := strings.Join(requests, ",")
	if !strings.Contains(joined, "tools/call:web_fetch") || !strings.Contains(joined, "tools/call:web_search") {
		t.Fatalf("missing MCP execution: %s", joined)
	}
	on, backend, _, _, _ := (&Manager{}).WebSearch()
	if on || backend != "ddgs" {
		t.Fatalf("search defaults changed: %v %s", on, backend)
	}
}
