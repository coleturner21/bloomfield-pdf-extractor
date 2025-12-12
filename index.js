import express from "express";
import pdfjsLib from "pdfjs-dist/legacy/build/pdf.js";

const app = express();
app.use(express.json({ limit: "50mb" }));

app.post("/extract", async (req, res) => {
  try {
    const { url } = req.body;
    if (!url) throw new Error("Missing PDF url");

    const pdfRes = await fetch(url);
    if (!pdfRes.ok) throw new Error("Failed to fetch PDF");

    const buffer = Buffer.from(await pdfRes.arrayBuffer());
    const pdf = await pdfjsLib.getDocument({ data: buffer }).promise;

    const pages = [];

    for (let i = 1; i <= pdf.numPages; i++) {
      const page = await pdf.getPage(i);
      const content = await page.getTextContent();

      pages.push(
        content.items.map((item) => ({
          text: item.str,
          x: item.transform[4],
          y: item.transform[5],
        }))
      );
    }

    res.json({ pageCount: pdf.numPages, pages });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log("PDF extractor running on port", PORT);
});
