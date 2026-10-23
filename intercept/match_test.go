package intercept

import (
	"database/sql"
	"strings"
	"testing"

	"github.com/Autumn-27/artex/db"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" driver used below
)

// closedDB returns a *db.DB whose connection is already closed, so any query
// against it fails immediately and deterministically with "sql: database is
// closed" — no network, no live Postgres, no flakiness. sql.Open itself never
// dials (the pgx stdlib driver connects lazily), so this is purely local.
func closedDB(t *testing.T) *db.DB {
	t.Helper()
	sqlDB, err := sql.Open("pgx", "postgres://closed-for-test/db")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	return &db.DB{DB: sqlDB}
}

// A rule-load failure must not be indistinguishable from "no rules
// configured": the latter is a deliberate, safe default (fall through to the
// judge, then allow); the former means the operator's own rules — including
// whatever scope they set up for the task — were never actually consulted.
// Silently allowing there would let a transient DB hiccup (a cold cache
// racing a pool hiccup right after a rule edit, say) drop the gate with no
// trace of which case happened. This pins the fail-safe behavior: Match must
// report a match (so the caller does not fall through to allow) with an
// Action that requires human approval, not a silent pass.
func TestMatchFailsSafeOnRuleReadError(t *testing.T) {
	i := &Interceptor{db: closedDB(t), pending: newPendingManager()}

	dec, matched := i.Match("Bash", []byte(`{"command":"whoami"}`))

	if !matched {
		t.Fatal("a rule-read error must report matched=true, so the caller does not fall through toward allow")
	}
	if dec.Action != "ask" {
		t.Fatalf("a rule-read error must require human approval (ask), got action %q", dec.Action)
	}
	if dec.Message == "" || !strings.Contains(dec.Message, "失败") {
		t.Fatalf("the message should say the read failed, not read like an ordinary rule hit: %q", dec.Message)
	}
}

// The legitimately-empty case — no rules configured at all — is a deliberate
// user state, not a failure, and must keep falling through exactly as before
// (matched=false, so the caller tries the judge, then allows).
func TestMatchNoMatchWhenNoRulesConfigured(t *testing.T) {
	i := &Interceptor{cached: []compiledRule{}, pending: newPendingManager()}

	dec, matched := i.Match("Bash", []byte(`{"command":"whoami"}`))

	if matched {
		t.Fatalf("an empty, successfully-loaded rule set must not match anything, got %+v", dec)
	}
}
