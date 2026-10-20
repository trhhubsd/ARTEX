package db

import (
	"encoding/json"
	"testing"
)

func TestParallelMCPSeedPreservesChoices(t *testing.T) {
	d, err := Open(testDSN(t))
	if err != nil {
		t.Fatal(err)
	}
	defer d.Close()
	tx, err := d.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	exec := func(q string) {
		t.Helper()
		if _, err := tx.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	seed := func() {
		t.Helper()
		if err := seedParallelMCP(tx); err != nil {
			t.Fatal(err)
		}
	}
	exec(`DELETE FROM mcp_servers WHERE name='parallel-search'`)
	exec(`DELETE FROM settings WHERE key='parallel_search_mcp_seed_v1'`)
	seed()
	var id int64
	var transport, url string
	var enabled, insecure bool
	var headers []byte
	if err := tx.QueryRow(`SELECT id,transport,url,env,enabled,insecure FROM mcp_servers WHERE name='parallel-search'`).Scan(&id, &transport, &url, &headers, &enabled, &insecure); err != nil {
		t.Fatal(err)
	}
	var env map[string]string
	if err := json.Unmarshal(headers, &env); err != nil {
		t.Fatal(err)
	}
	if transport != "http" || url != "https://search.parallel.ai/mcp" || enabled || insecure || len(env) != 1 || env["User-Agent"] != "ARTEX (+https://github.com/trhhubsd/ARTEX)" {
		t.Fatalf("unexpected preset: transport=%s url=%s enabled=%v insecure=%v headers=%v", transport, url, enabled, insecure, env)
	}
	var visible int
	if err := tx.QueryRow(`SELECT count(*) FROM agent_visibility WHERE resource_kind='mcp' AND resource_id=$1`, id).Scan(&visible); err != nil || visible != 0 {
		t.Fatalf("preset must be unassigned: count=%d err=%v", visible, err)
	}
	// Simulate an existing user server on upgrade, including configured auth.
	exec(`UPDATE mcp_servers SET url='https://example.invalid/custom',env='{"Authorization":"Bearer user-choice"}',enabled=true WHERE name='parallel-search'`)
	exec(`DELETE FROM settings WHERE key='parallel_search_mcp_seed_v1'`)
	seed()
	if err := tx.QueryRow(`SELECT url,env,enabled FROM mcp_servers WHERE name='parallel-search'`).Scan(&url, &headers, &enabled); err != nil {
		t.Fatal(err)
	}
	if url != "https://example.invalid/custom" || !enabled || string(headers) != `{"Authorization": "Bearer user-choice"}` {
		t.Fatalf("seed overwrote configured server: %s %s %v", url, headers, enabled)
	}
	exec(`UPDATE mcp_servers SET enabled=false WHERE name='parallel-search'`)
	seed()
	if err := tx.QueryRow(`SELECT enabled FROM mcp_servers WHERE name='parallel-search'`).Scan(&enabled); err != nil || enabled {
		t.Fatalf("seed re-enabled server: enabled=%v err=%v", enabled, err)
	}
	exec(`DELETE FROM mcp_servers WHERE name='parallel-search'`)
	seed()
	var count int
	if err := tx.QueryRow(`SELECT count(*) FROM mcp_servers WHERE name='parallel-search'`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("seed restored deleted server: count=%d err=%v", count, err)
	}
}
