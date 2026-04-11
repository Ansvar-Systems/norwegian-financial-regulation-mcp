# Norwegian Financial Regulation MCP

MCP server for Norwegian financial sector regulations -- Finanstilsynet forskrifter (regulations), rundskriv (circulars), veiledninger (guidance), and enforcement actions.

[![npm version](https://badge.fury.io/js/@ansvar%2Fnorwegian-financial-regulation-mcp.svg)](https://www.npmjs.com/package/@ansvar/norwegian-financial-regulation-mcp)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Covers Finanstilsynet (Norwegian Financial Supervisory Authority) regulatory publications with full-text search across provisions and enforcement actions. All data is in Norwegian.

Built by [Ansvar Systems](https://ansvar.eu) -- Stockholm, Sweden

---

## Regulator Covered

| Regulator | Role | Website |
|-----------|------|---------|
| **Finanstilsynet** (Norwegian Financial Supervisory Authority) | Supervision of banks, insurance companies, securities firms, payment institutions, auditors, and financial infrastructure | [finanstilsynet.no](https://www.finanstilsynet.no/) |

---

## Quick Start

### Use Remotely (No Install Needed)

**Endpoint:** `https://mcp.ansvar.eu/norwegian-financial-regulation/mcp`

| Client | How to Connect |
|--------|---------------|
| **Claude Desktop** | Add to `claude_desktop_config.json` (see below) |
| **Claude Code** | `claude mcp add norwegian-financial-regulation --transport http https://mcp.ansvar.eu/norwegian-financial-regulation/mcp` |

**Claude Desktop** -- add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "norwegian-financial-regulation": {
      "type": "url",
      "url": "https://mcp.ansvar.eu/norwegian-financial-regulation/mcp"
    }
  }
}
```

### Use Locally (npm)

```bash
npx @ansvar/norwegian-financial-regulation-mcp
```

Or add to Claude Desktop config for stdio:

```json
{
  "mcpServers": {
    "norwegian-financial-regulation": {
      "command": "npx",
      "args": ["-y", "@ansvar/norwegian-financial-regulation-mcp"]
    }
  }
}
```

---

## Tools

| Tool | Description |
|------|-------------|
| `no_fin_search_regulations` | Full-text search across Finanstilsynet forskrifter, rundskriv, and veiledninger |
| `no_fin_get_regulation` | Get a specific provision by sourcebook and reference |
| `no_fin_list_sourcebooks` | List all Finanstilsynet regulatory sourcebooks |
| `no_fin_search_enforcement` | Search enforcement actions -- fines, licence revocations, warnings |
| `no_fin_check_currency` | Check whether a provision is currently in force |
| `no_fin_about` | Return server metadata: version, data source, tool list |

Full tool documentation: [TOOLS.md](TOOLS.md)

---

## Data Coverage

| Sourcebook | Records | Content |
|------------|---------|---------|
| FTNO_FORSKRIFTER | 158 provisions | Capital adequacy, AML, insurance solvency, securities trading, payment services |
| FTNO_RUNDSKRIV | 109 provisions | ICT security, internal control, AML/CFT supervisory letters, risk management |
| FTNO_VEILEDNINGER | 106 provisions | Compliance guidance, outsourcing, fitness and propriety, reporting instructions |
| Enforcement actions | 110 actions | Administrative fines, licence revocations, restrictions, public warnings |
| **Total** | **486 records** | ~644 KB database |

**Language note:** All regulatory content is in Norwegian. Search queries work best in Norwegian (e.g., `hvitvaskingsloven`, `kapitaldekning`, `IKT-sikkerhet`, `overtredelsesgebyr`).

Full coverage details: [COVERAGE.md](COVERAGE.md)

---

## Data Sources

See [sources.yml](sources.yml) for machine-readable provenance metadata.

---

## Docker

```bash
docker build -t norwegian-financial-regulation-mcp .
docker run --rm -p 3000:3000 -v /path/to/data:/app/data norwegian-financial-regulation-mcp
```

Set `NO_FIN_DB_PATH` to use a custom database location (default: `data/no-fin.db`).

---

## Development

```bash
npm install
npm run build
npm run seed         # populate sample data
npm run dev          # HTTP server on port 3000
```

---

## Further Reading

- [TOOLS.md](TOOLS.md) -- full tool documentation with examples
- [COVERAGE.md](COVERAGE.md) -- data coverage and limitations
- [sources.yml](sources.yml) -- data provenance metadata
- [DISCLAIMER.md](DISCLAIMER.md) -- legal disclaimer
- [PRIVACY.md](PRIVACY.md) -- privacy policy
- [SECURITY.md](SECURITY.md) -- vulnerability disclosure

---

## License

Apache-2.0 -- [Ansvar Systems AB](https://ansvar.eu)

See [LICENSE](LICENSE) for the full license text.

See [DISCLAIMER.md](DISCLAIMER.md) for important legal disclaimers about the use of this regulatory data.

---

[ansvar.ai/mcp](https://ansvar.ai/mcp) -- Full MCP server catalog
