import express from "express";
import pdfjsLib from "pdfjs-dist/legacy/build/pdf.js";

const app = express();

// ---------- Config ----------
const MAX_PDF_BYTES = 40 * 1024 * 1024; // 40MB safety cap (tweak if needed)
const FETCH_TIMEOUT_MS = 45_000;        // 45s total fetch timeout
const PDF_PARSE_TIMEOUT_MS = 60_000;    // 60s parse budget (soft, per request)

// Keep JSON small and avoid base64 uploads
app.use(express.json({ limit: "256kb" }));

// ---------- Basic routes ----------
app.get("/", (req, res) => {
  res.type("text").send("Bloomfield PDF Extractor is running");
});

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

// ---------- Helpers ----------
function nowMs() {
  return Date.now();
}

function abortableFetch(url, timeoutMs) {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), timeoutMs);
  return fetch(url, { signal: controller.signal })
    .finally(() => clearTimeout(t));
}

function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function normalizeSpaces(s) {
  return String(s ?? "").replace(/\s+/g, " ").trim();
}

function isProbablyHeader(text) {
  const t = normalizeSpaces(text);
  if (!t) return false;
  if (t.length <= 3) return false;
  // Heuristics: short-ish, title-y, often uppercase
  const upperRatio = t.replace(/[^A-Za-z]/g, "").length
    ? (t.replace(/[^A-Z]/g, "").length / t.replace(/[^A-Za-z]/g, "").length)
    : 0;
  const hasColon = t.includes(":");
  const looksLikeTitle = t.length <= 60 && (upperRatio > 0.65 || hasColon);
  return looksLikeTitle;
}

/**
 * Group items into "lines" based on Y proximity.
 * pdfjs Y increases upward; we sort by Y descending so it reads top→bottom.
 */
function groupIntoLines(items, yTolerance = 2.5) {
  // Items: { text, x, y, w, h }
  const sorted = [...items].sort((a, b) => (b.y - a.y) || (a.x - b.x));

  const lines = [];
  for (const it of sorted) {
    let line = lines.find((l) => Math.abs(l.y - it.y) <= yTolerance);
    if (!line) {
      line = { y: it.y, items: [] };
      lines.push(line);
    }
    line.items.push(it);
  }

  // sort items left→right in each line
  for (const l of lines) {
    l.items.sort((a, b) => a.x - b.x);
  }

  // stable line order top→bottom
  lines.sort((a, b) => b.y - a.y);

  return lines;
}

/**
 * Split a line into columns by detecting big X-gaps.
 * Returns array of "cells" (strings).
 */
function splitLineIntoColumns(lineItems, gapMultiplier = 2.2) {
  // Estimate typical gap from character spacing / item widths
  const xs = lineItems.map((i) => i.x);
  if (xs.length <= 1) return [normalizeSpaces(lineItems.map(i => i.text).join(" "))];

  const gaps = [];
  for (let i = 1; i < lineItems.length; i++) {
    const prev = lineItems[i - 1];
    const cur = lineItems[i];
    const prevRight = prev.x + (prev.w ?? 0);
    gaps.push(cur.x - prevRight);
  }

  const positiveGaps = gaps.filter((g) => g > 0);
  const medianGap = positiveGaps.length
    ? positiveGaps.sort((a, b) => a - b)[Math.floor(positiveGaps.length / 2)]
    : 0;

  const threshold = Math.max(10, medianGap * gapMultiplier); // 10 px minimum

  const cells = [];
  let buf = [];
  for (let i = 0; i < lineItems.length; i++) {
    const cur = lineItems[i];
    buf.push(cur.text);

    const next = lineItems[i + 1];
    if (!next) break;

    const curRight = cur.x + (cur.w ?? 0);
    const gap = next.x - curRight;

    if (gap > threshold) {
      cells.push(normalizeSpaces(buf.join(" ")));
      buf = [];
    }
  }
  if (buf.length) cells.push(normalizeSpaces(buf.join(" ")));

  return cells;
}

/**
 * Decide if a group of consecutive lines looks like a table:
 * - many rows
 * - consistent number of columns (>=2)
 */
function detectTableLike(linesAsCells) {
  const rows = linesAsCells.filter(r => r.length >= 2 && r.some(c => c));
  if (rows.length < 4) return false;

  const colCounts = rows.map(r => r.length);
  const freq = new Map();
  for (const c of colCounts) freq.set(c, (freq.get(c) ?? 0) + 1);

  // most common column count
  let bestCount = null;
  let bestFreq = 0;
  for (const [k, v] of freq.entries()) {
    if (v > bestFreq) { bestFreq = v; bestCount = k; }
  }

  return bestCount !== null && bestCount >= 2 && bestFreq >= Math.max(3, Math.floor(rows.length * 0.6));
}

/**
 * Build blocks from lines:
 * - headers
 * - paragraphs (merged lines)
 * - tables (rows)
 */
function buildBlocksFromLines(lines) {
  // Convert each line into cells (columns)
  const linesAsCells = lines.map((l) => splitLineIntoColumns(l.items));

  const blocks = [];
  let i = 0;

  while (i < lines.length) {
    const cells = linesAsCells[i];
    const fullText = normalizeSpaces(cells.join(" "));

    // Skip empty
    if (!fullText) { i++; continue; }

    // Header
    if (isProbablyHeader(fullText)) {
      blocks.push({
        type: "header",
        text: fullText
      });
      i++;
      continue;
    }

    // Try table window
    const window = [];
    const windowCells = [];
    const start = i;

    // take up to ~25 lines as a candidate block
    for (let k = i; k < Math.min(lines.length, i + 25); k++) {
      window.push(lines[k]);
      windowCells.push(linesAsCells[k]);
      // stop early if we hit a header-like line (next section)
      if (k > i && isProbablyHeader(normalizeSpaces(windowCells[k - i].join(" ")))) break;
    }

    if (detectTableLike(windowCells)) {
      // Normalize table rows: keep consistent columns by padding
      const rowCount = windowCells.length;
      const maxCols = Math.max(...windowCells.map(r => r.length));

      const rows = windowCells
        .map(r => r.map(c => normalizeSpaces(c)))
        .map(r => {
          const padded = [...r];
          while (padded.length < maxCols) padded.push("");
          return padded;
        })
        // drop fully empty rows
        .filter(r => r.some(c => c));

      blocks.push({
        type: "table",
        rows,
        approx_row_count: rowCount
      });

      // advance i past the table-ish chunk
      i += windowCells.length;
      continue;
    }

    // Otherwise: build a paragraph by merging a few consecutive single/low-col lines
    const paraLines = [];
    let j = i;

    while (j < lines.length) {
      const t = normalizeSpaces(linesAsCells[j].join(" "));
      if (!t) { j++; continue; }
      if (isProbablyHeader(t) && paraLines.length > 0) break;

      // stop paragraph if we hit a very column-y line (likely table)
      if (linesAsCells[j].length >= 3 && paraLines.length > 0) break;

      paraLines.push(t);

      // don't let paragraphs run forever
      if (paraLines.length >= 8) break;
      j++;
    }

    blocks.push({
      type: "paragraph",
      text: normalizeSpaces(paraLines.join(" "))
    });

    i = Math.max(j, i + 1);
  }

  return blocks;
}

/**
 * Extract layout tokens from a pdfjs page
 */
async function extractPageTokens(page) {
  const content = await page.getTextContent();

  const tokens = [];
  for (const item of content.items) {
    const text = normalizeSpaces(item.str);
    if (!text) continue;

    // transform: [a, b, c, d, e, f] where e=x, f=y
    const tf = item.transform;
    const x = safeNum(tf?.[4]);
    const y = safeNum(tf?.[5]);

    // width/height exist on many builds; keep if present
    const w = safeNum(item.width);
    const h = safeNum(item.height);

    if (x === null || y === null) continue;

    tokens.push({ text, x, y, w: w ?? 0, h: h ?? 0 });
  }

  return tokens;
}

// ---------- Main extraction route ----------
app.post("/extract", async (req, res) => {
  const start = nowMs();

  try {
    const pdfUrl = req.body?.pdf_url;
    if (!pdfUrl || typeof pdfUrl !== "string") {
      return res.status(400).json({
        error: "Missing or invalid `pdf_url` (must be a string)."
      });
    }

    // Guard against extremely long requests
    const deadline = start + PDF_PARSE_TIMEOUT_MS;

    // Fetch PDF
    const pdfRes = await abortableFetch(pdfUrl, FETCH_TIMEOUT_MS);
    if (!pdfRes.ok) {
      return res.status(400).json({
        error: "Failed to fetch PDF",
        status: pdfRes.status
      });
    }

    const contentType = (pdfRes.headers.get("content-type") || "").toLowerCase();
    // Some signed URLs return application/octet-stream; allow that
    if (contentType && !contentType.includes("pdf") && !contentType.includes("octet-stream")) {
      return res.status(400).json({
        error: "URL did not return a PDF-like content-type",
        content_type: contentType
      });
    }

    const lenHeader = pdfRes.headers.get("content-length");
    const contentLength = lenHeader ? Number(lenHeader) : null;
    if (contentLength && contentLength > MAX_PDF_BYTES) {
      return res.status(413).json({
        error: "PDF too large",
        max_bytes: MAX_PDF_BYTES,
        content_length: contentLength
      });
    }

    const arrayBuffer = await pdfRes.arrayBuffer();
    if (arrayBuffer.byteLength > MAX_PDF_BYTES) {
      return res.status(413).json({
        error: "PDF too large",
        max_bytes: MAX_PDF_BYTES,
        content_length: arrayBuffer.byteLength
      });
    }

    // Parse PDF
    const pdfData = new Uint8Array(arrayBuffer);

    const loadingTask = pdfjsLib.getDocument({
      data: pdfData,
      // NOTE: In Node, pdfjs uses a "fake worker" internally but it is safe here (NOT Edge).
      disableFontFace: true,
      isEvalSupported: false
    });

    const pdf = await loadingTask.promise;

    const pages = [];
    for (let p = 1; p <= pdf.numPages; p++) {
      if (nowMs() > deadline) {
        return res.status(504).json({
          error: "Extraction timed out",
          processed_pages: p - 1,
          pageCount: pdf.numPages
        });
      }

      const page = await pdf.getPage(p);
      const tokens = await extractPageTokens(page);

      // Layout: lines -> blocks
      // tolerance: adjust if you see line-merging issues
      const lines = groupIntoLines(tokens, 2.8);
      const blocks = buildBlocksFromLines(lines);

      pages.push({
        page: p,
        block_count: blocks.length,
        blocks
      });
    }

    return res.json({
      meta: {
        pageCount: pdf.numPages,
        bytes: pdfData.byteLength,
        timing_ms: nowMs() - start,
        mode: "pdf_text_layer_layout",
        ocr_used: false
      },
      pages
    });
  } catch (err) {
    console.error("extract error:", err);
    return res.status(500).json({
      error: "Extraction failed",
      details: String(err)
    });
  }
});

// ---------- Start ----------
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("Bloomfield PDF extractor listening on", PORT);
});
