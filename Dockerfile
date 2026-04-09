# ─────────────────────────────────────────────────────────────────────────────
# Norwegian Financial Regulation MCP — multi-stage Dockerfile
# ─────────────────────────────────────────────────────────────────────────────
# Build:  docker build -t norwegian-financial-regulation-mcp .
# Run:    docker run --rm -p 3000:3000 norwegian-financial-regulation-mcp
#
# The image expects a pre-built database at /app/data/no-fin.db.
# Override with NO_FIN_DB_PATH for a custom location.
# ─────────────────────────────────────────────────────────────────────────────

# --- Stage 1: Build TypeScript + ingest data ---
FROM node:20-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends sqlite3 python3 build-essential curl bzip2 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm ci
COPY tsconfig.json ./
COPY src/ src/
COPY scripts/ scripts/
RUN npm run build
# Ingest real data from Finanstilsynet + Lovdata at build time
RUN npx tsx scripts/ingest-all.ts --force \
    && sqlite3 data/no-fin.db "PRAGMA journal_mode=DELETE;" \
    && rm -f data/no-fin.db-wal data/no-fin.db-shm

# --- Stage 2: Production ---
FROM node:20-slim AS production

WORKDIR /app
ENV NODE_ENV=production
ENV NO_FIN_DB_PATH=/app/data/no-fin.db

COPY package.json package-lock.json* ./
RUN npm ci --omit=dev && npm cache clean --force

COPY --from=builder /app/dist/ dist/
COPY --from=builder /app/data/no-fin.db data/no-fin.db

# Non-root user for security
RUN addgroup --system --gid 1001 mcp && \
    adduser --system --uid 1001 --ingroup mcp mcp && \
    chown -R mcp:mcp /app
USER mcp

# Health check: verify HTTP server responds
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD node -e "require('http').get('http://localhost:3000/health',r=>{process.exit(r.statusCode===200?0:1)}).on('error',()=>process.exit(1))"

CMD ["node", "dist/src/http-server.js"]
