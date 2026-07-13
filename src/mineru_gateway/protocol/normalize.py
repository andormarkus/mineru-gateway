"""Normalize MinerU output → Mistral OCR shape.

MinerU produces a "middle.json" (structured page data: blocks, text, bbox, images) and markdown. The Mistral /v1/ocr API
expects ``{pages: [{index, markdown, dimensions, images}]}``. This module converts between the two.

The MinerU middle-json structure (read from ../MinerU source):
  {"pdf_info": [ {page dicts}, ... ]}
Each page dict has ``page_idx``, ``page_size`` [w, h], and lists of blocks (paragraphs, images, tables) with ``bbox``
and content fields.
"""

from __future__ import annotations

import io
import json
import zipfile
from typing import Any

from mineru_gateway.protocol.ocr_models import OCRPage, OCRPageImage


def extract_from_zip(zip_bytes: bytes) -> dict[str, Any]:
    """Extract MinerU's output files from a result ZIP.

    Returns a dict of filename → content. Looks for ``middle.json`` (structured) and ``full.md`` / ``**.md``
    (markdown).
    """
    result: dict[str, Any] = {}
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        for name in zf.namelist():
            if name.endswith("/"):
                continue
            data = zf.read(name)
            if name.endswith("middle.json"):
                result["middle_json"] = json.loads(data)
            elif name.endswith(".md") and "full.md" not in result:
                result["markdown"] = data.decode("utf-8", errors="replace")
            elif name.endswith(".md"):
                result.setdefault("extra_md", []).append(data.decode("utf-8", errors="replace"))
    return result


def normalize_middle_json(middle: dict[str, Any]) -> list[OCRPage]:
    """Convert MinerU middle.json → list of OCRPage (Mistral shape).

    Robust to missing fields — a page with no recognizable blocks still yields a page with empty markdown.
    """
    pdf_info = middle.get("pdf_info") or middle.get("_pdf_info") or []
    return [_normalize_page(page, page_idx) for page_idx, page in enumerate(pdf_info)]


def _normalize_page(page: Any, page_idx: int) -> OCRPage:
    """Convert one MinerU page dict into an :class:`OCRPage`."""
    if isinstance(page, dict):
        idx = page.get("page_idx", page_idx)
        page_size = page.get("page_size") or page.get("size") or [0, 0]
    else:
        idx = page_idx
        page_size = [0, 0]

    dims = None
    if isinstance(page_size, (list, tuple)) and len(page_size) >= 2:
        dims = {"dpi": 72, "width": int(page_size[0]), "height": int(page_size[1])}

    md_parts: list[str] = []
    images: list[OCRPageImage] = []

    blocks = page.get("blocks") or page.get("para_blocks") or [] if isinstance(page, dict) else []
    for block in blocks:
        btype = block.get("type", "")
        text = _extract_block_text(block)
        if text:
            if btype in ("table", "image", "figure"):
                md_parts.append(f"\n\n{text}\n\n")
            else:
                md_parts.append(text)

        if btype in ("image", "figure") and include_images(block):
            img = _extract_image(block, len(images))
            if img:
                images.append(img)

    markdown = " ".join(md_parts).strip() if md_parts else ""
    return OCRPage(index=int(idx), markdown=markdown, images=images, dimensions=dims)


def normalize_markdown(markdown: str) -> list[OCRPage]:
    """Fallback: if only markdown is available (no middle.json), single page."""
    return [OCRPage(index=0, markdown=markdown, images=[], dimensions=None)]


def normalize_result(zip_bytes: bytes | None, *, markdown_fallback: str | None = None) -> list[OCRPage]:
    """Full normalization pipeline: ZIP → middle.json → OCRPages.

    If the ZIP lacks middle.json, falls back to markdown (single page).
    """
    if zip_bytes:
        try:
            extracted = extract_from_zip(zip_bytes)
            if "middle_json" in extracted:
                return normalize_middle_json(extracted["middle_json"])
            if "markdown" in extracted:
                return normalize_markdown(extracted["markdown"])
        except (zipfile.BadZipFile, json.JSONDecodeError):
            pass

    if markdown_fallback:
        return normalize_markdown(markdown_fallback)

    return [OCRPage(index=0, markdown="", images=[], dimensions=None)]


def _extract_block_text(block: dict[str, Any]) -> str:
    """Extract text from a MinerU block dict."""
    # MinerU blocks may have "lines" → "spans" → "content", or direct "text".
    lines = block.get("lines") or []
    parts: list[str] = []
    for line in lines:
        spans = line.get("spans") or []
        for span in spans:
            content = span.get("content") or span.get("text") or ""
            if content:
                parts.append(content)
    text = " ".join(parts).strip()
    if text:
        return text
    return (block.get("text") or block.get("content") or "").strip()


def include_images(block: dict[str, Any]) -> bool:
    """Whether a block has extractable image data."""
    return bool(block.get("blocks") or block.get("img_caption") or block.get("image_body"))


def _extract_image(block: dict[str, Any], idx: int) -> OCRPageImage | None:
    """Extract an OCRPageImage from a MinerU image block."""
    bbox = block.get("bbox") or [0, 0, 0, 0]
    return OCRPageImage(
        id=f"img-{idx}",
        top_left_x=int(bbox[0]) if len(bbox) > 0 else None,
        top_left_y=int(bbox[1]) if len(bbox) > 1 else None,
        bottom_right_x=int(bbox[2]) if len(bbox) > 2 else None,
        bottom_right_y=int(bbox[3]) if len(bbox) > 3 else None,
    )
