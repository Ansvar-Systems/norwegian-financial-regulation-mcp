#!/usr/bin/env node

/**
 * Norwegian Financial Regulation MCP -- stdio entry point.
 *
 * Provides MCP tools for querying Finanstilsynet (Norwegian FSA) regulations:
 * forskrifter (regulations), rundskriv (circulars),
 * veiledninger (guidance), and enforcement actions.
 *
 * Tool prefix: no_fin_
 */

import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { z, ZodError } from "zod";
import {
  listSourcebooks,
  searchProvisions,
  getProvision,
  searchEnforcement,
  checkProvisionCurrency,
} from "./db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback to default
}

const SERVER_NAME = "norwegian-financial-regulation-mcp";

// --- Tool definitions ---

const TOOLS = [
  {
    name: "no_fin_search_regulations",
    description:
      "Full-text search across Finanstilsynet regulatory provisions. Returns forskrifter (regulations), rundskriv (circulars), and veiledninger (guidance). Supports Norwegian-language queries.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query in Norwegian or English (e.g., 'hvitvaskingsloven', 'IKT-sikkerhet', 'kapitaldekning', 'Solvens II')",
        },
        sourcebook: {
          type: "string",
          description: "Filter by sourcebook ID (e.g., FTNO_FORSKRIFTER, FTNO_RUNDSKRIV, FTNO_VEILEDNINGER). Optional.",
        },
        status: {
          type: "string",
          enum: ["in_force", "deleted", "not_yet_in_force"],
          description: "Filter by provision status. Defaults to all statuses.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "no_fin_get_regulation",
    description:
      "Get a specific Finanstilsynet provision by sourcebook and reference.",
    inputSchema: {
      type: "object" as const,
      properties: {
        sourcebook: {
          type: "string",
          description: "Sourcebook identifier (e.g., FTNO_FORSKRIFTER, FTNO_RUNDSKRIV, FTNO_VEILEDNINGER)",
        },
        reference: {
          type: "string",
          description: "Full provision reference (e.g., 'Rundskriv 14/2021')",
        },
      },
      required: ["sourcebook", "reference"],
    },
  },
  {
    name: "no_fin_list_sourcebooks",
    description:
      "List all Finanstilsynet regulatory sourcebooks.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
  {
    name: "no_fin_search_enforcement",
    description:
      "Search Finanstilsynet enforcement actions — administrative fines, licence revocations, public warnings.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query (e.g., entity name, 'overtredelsesgebyr', 'tilbakekall')",
        },
        action_type: {
          type: "string",
          enum: ["fine", "ban", "restriction", "warning"],
          description: "Filter by action type. Optional.",
        },
        limit: {
          type: "number",
          description: "Maximum number of results to return. Defaults to 20.",
        },
      },
      required: ["query"],
    },
  },
  {
    name: "no_fin_check_currency",
    description:
      "Check whether a specific Finanstilsynet provision is currently in force.",
    inputSchema: {
      type: "object" as const,
      properties: {
        reference: {
          type: "string",
          description: "Full provision reference to check (e.g., 'Rundskriv 14/2021')",
        },
      },
      required: ["reference"],
    },
  },
  {
    name: "no_fin_about",
    description: "Return metadata about this MCP server: version, data source, tool list.",
    inputSchema: {
      type: "object" as const,
      properties: {},
      required: [],
    },
  },
];

// --- Zod schemas for argument validation ---

const SearchRegulationsArgs = z.object({
  query: z.string().min(1),
  sourcebook: z.string().optional(),
  status: z.enum(["in_force", "deleted", "not_yet_in_force"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetRegulationArgs = z.object({
  sourcebook: z.string().min(1),
  reference: z.string().min(1),
});

const SearchEnforcementArgs = z.object({
  query: z.string().min(1),
  action_type: z.enum(["fine", "ban", "restriction", "warning"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const CheckCurrencyArgs = z.object({
  reference: z.string().min(1),
});

// --- Helper ---

const RESPONSE_META = {
  disclaimer:
    "For informational purposes only. Not legal advice. Verify with official Finanstilsynet publications at finanstilsynet.no.",
  data_age: "2026-04-04",
  copyright: "© Finanstilsynet. Data sourced from finanstilsynet.no.",
  source_url: "https://www.finanstilsynet.no/",
};

function responseMeta() {
  return RESPONSE_META;
}

function addCitation(
  item: Record<string, unknown>,
  lookup: string,
): Record<string, unknown> {
  const sourcebook = (item.sourcebook as string) ?? "";
  const reference = (item.reference as string) ?? (item.id as string) ?? "";
  return {
    ...item,
    _citation: {
      canonical_ref: sourcebook ? `${sourcebook}::${reference}` : reference,
      display_text: `Finanstilsynet: ${reference || sourcebook}`,
      lookup,
    },
  };
}

function textContent(data: unknown) {
  const payload =
    typeof data === "object" && data !== null
      ? { ...(data as object), _meta: responseMeta() }
      : { data, _meta: responseMeta() };
  return {
    content: [
      { type: "text" as const, text: JSON.stringify(payload, null, 2) },
    ],
  };
}

function errorContent(
  message: string,
  errorType: "not_found" | "invalid_input" | "internal_error" = "internal_error",
) {
  return {
    content: [
      {
        type: "text" as const,
        text: JSON.stringify(
          { error: message, _error_type: errorType, _meta: responseMeta() },
          null,
          2,
        ),
      },
    ],
    isError: true as const,
  };
}

// --- Server setup ---

const server = new Server(
  { name: SERVER_NAME, version: pkgVersion },
  { capabilities: { tools: {} } },
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: TOOLS,
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args = {} } = request.params;

  try {
    switch (name) {
      case "no_fin_search_regulations": {
        const parsed = SearchRegulationsArgs.parse(args);
        const results = searchProvisions({
          query: parsed.query,
          sourcebook: parsed.sourcebook,
          status: parsed.status,
          limit: parsed.limit,
        });
        const citedResults = (results as Record<string, unknown>[]).map((r) =>
          addCitation(r, "no_fin_get_regulation"),
        );
        return textContent({ results: citedResults, count: results.length });
      }

      case "no_fin_get_regulation": {
        const parsed = GetRegulationArgs.parse(args);
        const provision = getProvision(parsed.sourcebook, parsed.reference);
        if (!provision) {
          return errorContent(
            `Provision not found: ${parsed.sourcebook} ${parsed.reference}`,
            "not_found",
          );
        }
        return textContent(
          addCitation(provision as Record<string, unknown>, "no_fin_get_regulation"),
        );
      }

      case "no_fin_list_sourcebooks": {
        const sourcebooks = listSourcebooks();
        return textContent({ sourcebooks, count: sourcebooks.length });
      }

      case "no_fin_search_enforcement": {
        const parsed = SearchEnforcementArgs.parse(args);
        const results = searchEnforcement({
          query: parsed.query,
          action_type: parsed.action_type,
          limit: parsed.limit,
        });
        const citedResults = (results as Record<string, unknown>[]).map((r) =>
          addCitation(r, "no_fin_search_enforcement"),
        );
        return textContent({ results: citedResults, count: results.length });
      }

      case "no_fin_check_currency": {
        const parsed = CheckCurrencyArgs.parse(args);
        const currency = checkProvisionCurrency(parsed.reference);
        return textContent(currency);
      }

      case "no_fin_about": {
        return textContent({
          name: SERVER_NAME,
          version: pkgVersion,
          description:
            "Finanstilsynet (Norwegian FSA) financial regulation MCP server. Provides access to forskrifter (regulations), rundskriv (circulars), veiledninger (guidance), and enforcement actions.",
          data_source: "Finanstilsynet regulatory publications (https://www.finanstilsynet.no/)",
          tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
        });
      }

      default:
        return errorContent(`Unknown tool: ${name}`, "invalid_input");
    }
  } catch (err) {
    if (err instanceof ZodError) {
      return errorContent(`Invalid input for ${name}: ${err.message}`, "invalid_input");
    }
    const message = err instanceof Error ? err.message : String(err);
    return errorContent(`Error in ${name}: ${message}`, "internal_error");
  }
});

// --- Main ---

async function main(): Promise<void> {
  const transport = new StdioServerTransport();
  await server.connect(transport);
  process.stderr.write(`${SERVER_NAME} v${pkgVersion} running on stdio\n`);
}

main().catch((err) => {
  process.stderr.write(`Fatal error: ${err instanceof Error ? err.message : String(err)}\n`);
  process.exit(1);
});
