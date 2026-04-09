/**
 * Full real-data ingestion for the Finanstilsynet (Norway) MCP server.
 *
 * Fetches verified regulatory data from:
 *   - finanstilsynet.no API (/api/search/nyhetsarkiv) — rundskriv, veiledninger
 *   - finanstilsynet.no individual pages — full text of each rundskriv/veiledning
 *   - finanstilsynet.no API (type 104) — tilsynsrapporter og vedtak (enforcement)
 *   - lovdata.no — key financial forskrifter (regulations)
 *
 * Usage:
 *   npx tsx scripts/ingest-all.ts
 *   npx tsx scripts/ingest-all.ts --force   # drop and recreate
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["NO_FIN_DB_PATH"] ?? "data/no-fin.db";
const force = process.argv.includes("--force");

// Rate limiting: polite delay between requests
const DELAY_MS = 800;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

// ── Bootstrap database ─────────────────────────────────────────────────────

const dir = dirname(DB_PATH);
if (!existsSync(dir)) {
  mkdirSync(dir, { recursive: true });
}

if (force && existsSync(DB_PATH)) {
  unlinkSync(DB_PATH);
  console.log(`Deleted existing database at ${DB_PATH}`);
}

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);

console.log(`Database initialised at ${DB_PATH}`);

// ── Sourcebooks ─────────────────────────────────────────────────────────────

const sourcebooks = [
  {
    id: "FTNO_FORSKRIFTER",
    name: "Finanstilsynet Forskrifter (Regulations/Ordinances)",
    description:
      "Binding regulations (forskrifter) issued under Norwegian financial legislation, sourced from Lovdata. Covers capital adequacy, risk management, governance, AML/CFT, reporting, and prudential requirements.",
  },
  {
    id: "FTNO_RUNDSKRIV",
    name: "Finanstilsynet Rundskriv (Circulars)",
    description:
      "Circulars (rundskriv) issued by Finanstilsynet communicating supervisory expectations, interpretive guidance, and practice standards. Finanstilsynet announced in 2025 that veiledninger will replace rundskriv going forward.",
  },
  {
    id: "FTNO_VEILEDNINGER",
    name: "Finanstilsynet Veiledninger (Guidance)",
    description:
      "Non-binding guidance documents (veiledninger) published by Finanstilsynet explaining the application and interpretation of Norwegian and EEA financial regulation.",
  },
];

const insertSourcebook = db.prepare(
  "INSERT OR IGNORE INTO sourcebooks (id, name, description) VALUES (?, ?, ?)",
);

for (const sb of sourcebooks) {
  insertSourcebook.run(sb.id, sb.name, sb.description);
}

console.log(`Inserted ${sourcebooks.length} sourcebooks`);

// ── Types ───────────────────────────────────────────────────────────────────

interface ApiHit {
  id: number;
  name: string;
  url: string;
  published: string;
  preamble: string;
  metaData: string;
}

interface ApiResponse {
  hits: ApiHit[];
  page: number;
  totalHits: number;
  totalPages: number;
  pageSize: number;
}

// ── Helpers ─────────────────────────────────────────────────────────────────

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "AnsvarMCP/1.0 (compliance research; https://ansvar.eu)",
      Accept: "application/json",
    },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return (await res.json()) as T;
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetch(url, {
    headers: {
      "User-Agent": "AnsvarMCP/1.0 (compliance research; https://ansvar.eu)",
      Accept: "text/html",
    },
  });
  if (!res.ok) {
    throw new Error(`HTTP ${res.status} for ${url}`);
  }
  return res.text();
}

/**
 * Parse Norwegian date format "DD. MMMM YYYY" to ISO "YYYY-MM-DD".
 */
function parseNorwegianDate(dateStr: string): string {
  const months: Record<string, string> = {
    januar: "01",
    februar: "02",
    mars: "03",
    april: "04",
    mai: "05",
    juni: "06",
    juli: "07",
    august: "08",
    september: "09",
    oktober: "10",
    november: "11",
    desember: "12",
  };

  const cleaned = dateStr.replace(/\./g, "").trim();
  const parts = cleaned.split(/\s+/);
  if (parts.length < 3) return dateStr;

  const day = (parts[0] ?? "").padStart(2, "0");
  const monthName = (parts[1] ?? "").toLowerCase();
  const year = parts[2] ?? "";
  const month = months[monthName];

  if (!month || !year) return dateStr;
  return `${year}-${month}-${day}`;
}

/**
 * Extract main text content from a Finanstilsynet page HTML.
 * Strips HTML tags but preserves paragraph structure.
 */
function extractPageText(html: string): string {
  let content = html;

  // Try to isolate the article body
  const articleMatch = content.match(
    /<article[^>]*>([\s\S]*?)<\/article>/i,
  );
  if (articleMatch?.[1]) {
    content = articleMatch[1];
  } else {
    // Fallback: grab the main content area
    const mainMatch = content.match(
      /<main[^>]*>([\s\S]*?)<\/main>/i,
    );
    if (mainMatch?.[1]) {
      content = mainMatch[1];
    }
  }

  // Remove script and style tags
  content = content.replace(/<script[^>]*>[\s\S]*?<\/script>/gi, "");
  content = content.replace(/<style[^>]*>[\s\S]*?<\/style>/gi, "");

  // Convert block elements to newlines
  content = content.replace(/<\/?(p|div|br|h[1-6]|li|tr)[^>]*>/gi, "\n");

  // Remove all remaining HTML tags
  content = content.replace(/<[^>]+>/g, "");

  // Decode HTML entities
  content = content
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, " ")
    .replace(/&#\d+;/g, "");

  // Clean up whitespace
  content = content
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .join("\n");

  return content.trim();
}

/**
 * Extract the reference number from metaData field.
 * Examples: "Rundskriv 4/2022", "Veiledning", "Rundskriv/veiledninger 15/2019"
 */
function parseMetaData(
  metaData: string,
  name: string,
): { type: "rundskriv" | "veiledning"; reference: string } {
  if (metaData === "Veiledning") {
    return { type: "veiledning", reference: name };
  }

  // "Rundskriv X/YYYY" or "Rundskriv/veiledninger X/YYYY"
  const match = metaData.match(
    /(?:Rundskriv(?:\/veiledninger)?)\s+(\d+\/\d{4})/,
  );
  if (match?.[1]) {
    return { type: "rundskriv", reference: `Rundskriv ${match[1]}` };
  }

  // Fallback: treat as veiledning with the name as reference
  return { type: "veiledning", reference: name };
}

// ── Fetch all rundskriv/veiledninger ────────────────────────────────────────

async function fetchAllRundskrivVeiledninger(): Promise<void> {
  console.log("\n--- Fetching rundskriv/veiledninger from Finanstilsynet API ---");

  const baseUrl =
    "https://www.finanstilsynet.no/api/search/nyhetsarkiv?query=&t=59&language=no";

  // Get first page to know total
  const firstPage = await fetchJson<ApiResponse>(`${baseUrl}&page=1`);
  console.log(
    `Total: ${firstPage.totalHits} items across ${firstPage.totalPages} pages`,
  );

  const allHits: ApiHit[] = [...firstPage.hits];

  // Fetch remaining pages
  for (let page = 2; page <= firstPage.totalPages; page++) {
    await sleep(DELAY_MS);
    const pageData = await fetchJson<ApiResponse>(`${baseUrl}&page=${page}`);
    allHits.push(...pageData.hits);
    console.log(`  Page ${page}/${firstPage.totalPages}: ${pageData.hits.length} items`);
  }

  console.log(`Fetched ${allHits.length} index entries. Now fetching full text...`);

  const insertProvision = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  let failed = 0;

  for (const hit of allHits) {
    const { type, reference } = parseMetaData(hit.metaData, hit.name);
    const sourcebookId =
      type === "rundskriv" ? "FTNO_RUNDSKRIV" : "FTNO_VEILEDNINGER";

    // Clean the URL — strip tracking parameters
    const cleanUrl = hit.url.split("?")[0] ?? hit.url;

    let fullText = hit.preamble; // Fallback to preamble

    try {
      await sleep(DELAY_MS);
      const html = await fetchHtml(cleanUrl);
      const extracted = extractPageText(html);
      if (extracted.length > fullText.length) {
        fullText = extracted;
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Warning: Could not fetch full text for "${hit.name}": ${msg}`);
    }

    const effectiveDate = parseNorwegianDate(hit.published);

    try {
      insertProvision.run(
        sourcebookId,
        reference,
        hit.name,
        fullText,
        type,
        "in_force",
        effectiveDate,
        "", // chapter
        "", // section
      );
      inserted++;
      if (inserted % 10 === 0) {
        console.log(`  Inserted ${inserted}/${allHits.length} provisions...`);
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Error inserting "${hit.name}": ${msg}`);
      failed++;
    }
  }

  console.log(
    `Rundskriv/veiledninger: ${inserted} inserted, ${failed} failed`,
  );
}

// ── Fetch enforcement actions ───────────────────────────────────────────────

async function fetchEnforcementActions(): Promise<void> {
  console.log("\n--- Fetching enforcement actions from Finanstilsynet API ---");

  const baseUrl =
    "https://www.finanstilsynet.no/api/search/nyhetsarkiv?query=&t=104&language=no";

  const firstPage = await fetchJson<ApiResponse>(`${baseUrl}&page=1`);
  console.log(
    `Total: ${firstPage.totalHits} enforcement reports across ${firstPage.totalPages} pages`,
  );

  // Fetch ALL pages
  const allHits: ApiHit[] = [...firstPage.hits];

  for (let page = 2; page <= firstPage.totalPages; page++) {
    await sleep(DELAY_MS);
    const pageData = await fetchJson<ApiResponse>(`${baseUrl}&page=${page}`);
    allHits.push(...pageData.hits);
    console.log(`  Page ${page}/${firstPage.totalPages}: ${pageData.hits.length} items`);
  }

  console.log(`Fetched ${allHits.length} enforcement entries`);

  const insertEnforcement = db.prepare(`
    INSERT INTO enforcement_actions (firm_name, reference_number, action_type, amount, date, summary, sourcebook_references)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;

  const insertAll = db.transaction(() => {
    for (const hit of allHits) {
      const lowerPreamble = (hit.preamble + " " + hit.name).toLowerCase();
      let actionType = "inspection";
      if (
        lowerPreamble.includes("overtredelsesgebyr") ||
        lowerPreamble.includes("gebyr")
      ) {
        actionType = "fine";
      } else if (
        lowerPreamble.includes("tilbakekall") ||
        lowerPreamble.includes("inndragning")
      ) {
        actionType = "ban";
      } else if (
        lowerPreamble.includes("advarsel") ||
        lowerPreamble.includes("åtvar")
      ) {
        actionType = "warning";
      } else if (
        lowerPreamble.includes("pålegg") ||
        lowerPreamble.includes("vilkår")
      ) {
        actionType = "restriction";
      }

      const date = parseNorwegianDate(hit.published);

      insertEnforcement.run(
        hit.name,
        `FTNO/${hit.id}`,
        actionType,
        0,
        date,
        hit.preamble,
        "",
      );
      inserted++;
    }
  });

  insertAll();
  console.log(`Enforcement actions: ${inserted} inserted`);
}

// ── Fetch ALL Finanstilsynet forskrifter from Lovdata public dataset ────────

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

const LOVDATA_DATASET_URL =
  "https://api.lovdata.no/v1/publicData/get/gjeldende-sentrale-forskrifter.tar.bz2";

async function fetchForskrifter(): Promise<void> {
  console.log("\n--- Fetching ALL Finanstilsynet forskrifter from Lovdata public dataset ---");

  // Download and extract the complete forskrifter dataset
  const tmpDir = "/tmp/lovdata-forskrifter";
  const tarFile = "/tmp/lovdata-forskrifter.tar.bz2";

  console.log("  Downloading Lovdata public dataset (~21MB)...");
  const response = await fetch(LOVDATA_DATASET_URL, {
    headers: {
      "User-Agent": "AnsvarMCP/1.0 (compliance research; https://ansvar.eu)",
    },
  });
  if (!response.ok) {
    throw new Error(`Failed to download Lovdata dataset: HTTP ${response.status}`);
  }
  const buffer = Buffer.from(await response.arrayBuffer());
  writeFileSync(tarFile, buffer);
  console.log(`  Downloaded ${(buffer.length / 1024 / 1024).toFixed(1)}MB`);

  execFileSync("rm", ["-rf", tmpDir]);
  mkdirSync(tmpDir, { recursive: true });
  execFileSync("tar", ["xjf", tarFile, "-C", tmpDir]);

  const sfDir = join(tmpDir, "sf");
  const xmlFiles = readdirSync(sfDir).filter((f) => f.endsWith(".xml"));
  console.log(`  Dataset contains ${xmlFiles.length} forskrifter total`);

  // Find all forskrifter where Finanstilsynet is listed as subunit (etat)
  const insertProvision = db.prepare(`
    INSERT INTO provisions (sourcebook_id, reference, title, text, type, status, effective_date, chapter, section)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let inserted = 0;
  let skipped = 0;

  for (const fname of xmlFiles) {
    const filePath = join(sfDir, fname);
    const content = readFileSync(filePath, "utf-8");

    // Only include forskrifter where Finanstilsynet is the administering subunit
    if (!content.includes("<li>Finanstilsynet</li>") || !content.includes("subunit")) {
      continue;
    }

    // Extract metadata from the HTML/XML header
    const refMatch = content.match(/<dd class="legacyID">(FOR-[^<]+)/);
    const titleMatch = content.match(/<title>([^<]+)/);
    const dateMatch = content.match(/<dd class="dateInForce">([^<]+)/);

    const reference = refMatch?.[1] ?? fname.replace("sf-", "FOR-").replace(".xml", "");
    const title = titleMatch?.[1] ?? "Untitled forskrift";
    const effectiveDate = dateMatch?.[1] ?? "";

    // Extract full text content
    let fullText = extractPageText(content);

    // Truncate very long forskrifter to keep DB size reasonable
    if (fullText.length > 50000) {
      const lovdataUrl = `https://lovdata.no/dokument/SF/forskrift/${reference.replace("FOR-", "")}`;
      fullText =
        fullText.slice(0, 50000) +
        "\n\n[Truncated -- full text at " +
        lovdataUrl +
        "]";
    }

    if (fullText.length < 50) {
      console.warn(`  Skipping ${reference}: too short (${fullText.length} chars)`);
      skipped++;
      continue;
    }

    try {
      insertProvision.run(
        "FTNO_FORSKRIFTER",
        reference,
        title,
        fullText,
        "forskrift",
        "in_force",
        effectiveDate,
        "",
        "",
      );
      inserted++;
      console.log(`  [${inserted}] ${reference}: ${title}`);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      console.warn(`  Error inserting ${reference}: ${msg}`);
      skipped++;
    }
  }

  // Clean up
  execFileSync("rm", ["-rf", tmpDir, tarFile]);

  console.log(`Forskrifter: ${inserted} inserted, ${skipped} skipped`);
}

// ── Update coverage.json ────────────────────────────────────────────────────

function updateCoverage(): void {
  const provisionCount = (
    db.prepare("SELECT count(*) as cnt FROM provisions").get() as {
      cnt: number;
    }
  ).cnt;
  const sourcebookCount = (
    db.prepare("SELECT count(*) as cnt FROM sourcebooks").get() as {
      cnt: number;
    }
  ).cnt;
  const enforcementCount = (
    db.prepare("SELECT count(*) as cnt FROM enforcement_actions").get() as {
      cnt: number;
    }
  ).cnt;
  const ftsCount = (
    db.prepare("SELECT count(*) as cnt FROM provisions_fts").get() as {
      cnt: number;
    }
  ).cnt;

  const rundskrivCount = (
    db
      .prepare(
        "SELECT count(*) as cnt FROM provisions WHERE sourcebook_id = 'FTNO_RUNDSKRIV'",
      )
      .get() as { cnt: number }
  ).cnt;
  const veiledningCount = (
    db
      .prepare(
        "SELECT count(*) as cnt FROM provisions WHERE sourcebook_id = 'FTNO_VEILEDNINGER'",
      )
      .get() as { cnt: number }
  ).cnt;
  const forskriftCount = (
    db
      .prepare(
        "SELECT count(*) as cnt FROM provisions WHERE sourcebook_id = 'FTNO_FORSKRIFTER'",
      )
      .get() as { cnt: number }
  ).cnt;

  const now = new Date().toISOString().split("T")[0]!;

  const coverage = {
    schema_version: "1.0",
    mcp_name: "Norwegian Financial Regulation MCP",
    mcp_type: "domain_intelligence",
    coverage_date: now,
    database_version: "1.0.0",
    sources: [
      {
        id: "ftno_forskrifter",
        name: "Finanstilsynet Forskrifter (Regulations)",
        authority: "Lovdata / Finansdepartementet",
        url: "https://lovdata.no",
        version: "Current in-force regulations",
        item_count: forskriftCount,
        item_type: "provision",
        last_refresh: now,
        refresh_frequency: "quarterly",
        completeness: "partial",
        completeness_note: `${forskriftCount} key financial regulations from Lovdata covering CRR/CRD, AML, insurance, securities, real estate, and payment services.`,
      },
      {
        id: "ftno_rundskriv",
        name: "Finanstilsynet Rundskriv (Circulars)",
        authority: "Finanstilsynet",
        url: "https://www.finanstilsynet.no/nyhetsarkiv/rundskriv/",
        version: "All current circulars",
        item_count: rundskrivCount,
        item_type: "provision",
        last_refresh: now,
        refresh_frequency: "quarterly",
        completeness: "full",
        completeness_note: `${rundskrivCount} rundskriv -- complete set from Finanstilsynet public archive.`,
      },
      {
        id: "ftno_veiledninger",
        name: "Finanstilsynet Veiledninger (Guidance)",
        authority: "Finanstilsynet",
        url: "https://www.finanstilsynet.no/nyhetsarkiv/rundskriv/",
        version: "All current guidance documents",
        item_count: veiledningCount,
        item_type: "provision",
        last_refresh: now,
        refresh_frequency: "quarterly",
        completeness: "full",
        completeness_note: `${veiledningCount} veiledninger -- complete set from Finanstilsynet public archive.`,
      },
      {
        id: "enforcement_actions",
        name: "Finanstilsynet Tilsynsrapporter og Vedtak",
        authority: "Finanstilsynet",
        url: "https://www.finanstilsynet.no/nyhetsarkiv/tilsynsrapporter/",
        version: "Recent enforcement decisions and inspection reports",
        item_count: enforcementCount,
        item_type: "enforcement_action",
        last_refresh: now,
        refresh_frequency: "quarterly",
        completeness: "partial",
        completeness_note: `${enforcementCount} most recent inspection reports and enforcement decisions.`,
      },
    ],
    gaps: [
      {
        id: "forskrift-full-corpus",
        description:
          "Only key financial forskrifter included (not the full Lovdata corpus)",
        reason: "Focused on Finanstilsynet-administered regulations",
        impact: "medium",
        planned: true,
        target_version: "1.1",
      },
      {
        id: "enforcement-full-text",
        description:
          "Enforcement actions contain summaries, not full inspection reports",
        reason: "Full reports require individual page scraping",
        impact: "medium",
        planned: true,
        target_version: "1.1",
      },
      {
        id: "finansklagenemnda",
        description:
          "Finansklagenemnda (Financial Complaints Board) decisions",
        reason: "Separate data source requiring dedicated ingestion",
        impact: "medium",
        planned: true,
        target_version: "1.2",
      },
      {
        id: "repealed-regulations",
        description: "Historical and repealed regulations",
        reason: "Focus on current in-force regulations",
        impact: "low",
        planned: false,
      },
    ],
    tools: [
      {
        name: "no_fin_search_regulations",
        category: "search",
        description:
          "Full-text search across Finanstilsynet forskrifter, rundskriv, and veiledninger",
        data_sources: [
          "ftno_forskrifter",
          "ftno_rundskriv",
          "ftno_veiledninger",
        ],
        verified: true,
      },
      {
        name: "no_fin_get_regulation",
        category: "lookup",
        description:
          "Retrieve a specific provision by sourcebook and reference",
        data_sources: [
          "ftno_forskrifter",
          "ftno_rundskriv",
          "ftno_veiledninger",
        ],
        verified: true,
      },
      {
        name: "no_fin_list_sourcebooks",
        category: "meta",
        description: "List all Finanstilsynet regulatory sourcebooks",
        data_sources: [],
        verified: true,
      },
      {
        name: "no_fin_search_enforcement",
        category: "search",
        description: "Search Finanstilsynet enforcement actions",
        data_sources: ["enforcement_actions"],
        verified: true,
      },
      {
        name: "no_fin_check_currency",
        category: "lookup",
        description: "Check whether a provision is currently in force",
        data_sources: [
          "ftno_forskrifter",
          "ftno_rundskriv",
          "ftno_veiledninger",
        ],
        verified: true,
      },
      {
        name: "no_fin_about",
        category: "meta",
        description: "Return server metadata, data source, tool list",
        data_sources: [],
        verified: true,
      },
    ],
    summary: {
      total_tools: 6,
      total_sources: 4,
      total_items: 0,
      db_size_mb: 0,
      known_gaps: 4,
      gaps_planned: 3,
    },
  };

  coverage.summary.total_items = provisionCount + enforcementCount;

  writeFileSync(
    "data/coverage.json",
    JSON.stringify(coverage, null, 2) + "\n",
  );

  console.log(`\nDatabase summary:`);
  console.log(`  Sourcebooks:          ${sourcebookCount}`);
  console.log(`  Provisions:           ${provisionCount}`);
  console.log(`    Forskrifter:        ${forskriftCount}`);
  console.log(`    Rundskriv:          ${rundskrivCount}`);
  console.log(`    Veiledninger:       ${veiledningCount}`);
  console.log(`  Enforcement actions:  ${enforcementCount}`);
  console.log(`  FTS entries:          ${ftsCount}`);
  console.log(`  Total items:          ${provisionCount + enforcementCount}`);
}

// ── Main ────────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  console.log(
    "Starting real data ingestion from Finanstilsynet + Lovdata...\n",
  );

  await fetchAllRundskrivVeiledninger();
  await fetchEnforcementActions();
  await fetchForskrifter();

  updateCoverage();

  db.close();
  console.log(`\nDone. Database ready at ${DB_PATH}`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  db.close();
  process.exit(1);
});
