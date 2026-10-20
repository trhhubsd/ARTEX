package db

import "database/sql"

// seedParallelMCP offers public documentation search/fetch through the existing
// remote MCP loader. It is disabled and has no agent visibility until opted in.
// The marker and insert share one statement: edits, existing same-name servers,
// and explicit deletion survive restart, including when upgrading an old DB.
func seedParallelMCP(d interface {
	Exec(string, ...any) (sql.Result, error)
}) error {
	_, err := d.Exec(`
WITH first_seed AS (
    INSERT INTO settings(key, value) VALUES ('parallel_search_mcp_seed_v1', 'done')
    ON CONFLICT (key) DO NOTHING RETURNING key
)
INSERT INTO mcp_servers(name, transport, url, env, enabled)
SELECT 'parallel-search', 'http', 'https://search.parallel.ai/mcp',
       '{"User-Agent":"ARTEX (+https://github.com/trhhubsd/ARTEX)"}'::jsonb, false
FROM first_seed
ON CONFLICT (name) DO NOTHING`)
	return err
}
