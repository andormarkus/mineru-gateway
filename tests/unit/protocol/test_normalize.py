"""Phase 5 tests: MinerU → Mistral normalization.

Tests that middle.json and markdown inputs normalize to the correct
{pages: [...]} Mistral shape.
"""

from __future__ import annotations

import io
import json
import zipfile

from mineru_gateway.protocol.normalize import (
    extract_from_zip,
    normalize_markdown,
    normalize_middle_json,
    normalize_result,
)


def _make_zip(middle_json: dict | None = None, markdown: str | None = None) -> bytes:
    """Build a MinerU-style result ZIP."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        if middle_json is not None:
            zf.writestr("taskid/middle.json", json.dumps(middle_json))
        if markdown is not None:
            zf.writestr("taskid/full.md", markdown)
    return buf.getvalue()


def test_normalize_markdown_single_page() -> None:
    pages = normalize_markdown("# Hello\n\nWorld")
    assert len(pages) == 1
    assert pages[0].index == 0
    assert "Hello" in pages[0].markdown


def test_normalize_middle_json_basic() -> None:
    """Two pages with text blocks → two OCRPages."""
    middle = {
        "pdf_info": [
            {
                "page_idx": 0,
                "page_size": [612, 792],
                "blocks": [{"type": "text", "lines": [{"spans": [{"content": "Page one text"}]}]}],
            },
            {
                "page_idx": 1,
                "page_size": [612, 792],
                "blocks": [{"type": "text", "lines": [{"spans": [{"content": "Page two text"}]}]}],
            },
        ]
    }
    pages = normalize_middle_json(middle)
    assert len(pages) == 2
    assert pages[0].index == 0
    assert "Page one text" in pages[0].markdown
    assert pages[1].index == 1
    assert "Page two text" in pages[1].markdown
    assert pages[0].dimensions == {"dpi": 72, "width": 612, "height": 792}


def test_normalize_empty_pages() -> None:
    """A page with no blocks yields an empty-markdown page, not a crash."""
    middle = {"pdf_info": [{"page_idx": 0, "page_size": [100, 100]}]}
    pages = normalize_middle_json(middle)
    assert len(pages) == 1
    assert pages[0].markdown == ""


def test_normalize_result_from_zip() -> None:
    """Full pipeline: ZIP with middle.json → pages."""
    middle = {
        "pdf_info": [
            {
                "page_idx": 0,
                "page_size": [200, 300],
                "blocks": [{"type": "text", "lines": [{"spans": [{"content": "Extracted"}]}]}],
            }
        ]
    }
    zip_bytes = _make_zip(middle_json=middle)
    pages = normalize_result(zip_bytes)
    assert len(pages) == 1
    assert "Extracted" in pages[0].markdown


def test_normalize_result_markdown_fallback() -> None:
    """ZIP with only markdown (no middle.json) → single page."""
    zip_bytes = _make_zip(markdown="# Fallback")
    pages = normalize_result(zip_bytes)
    assert len(pages) == 1
    assert "Fallback" in pages[0].markdown


def test_normalize_result_empty() -> None:
    """No input → single empty page (graceful degradation)."""
    pages = normalize_result(None)
    assert len(pages) == 1
    assert pages[0].markdown == ""


def test_normalize_result_bad_zip() -> None:
    """Garbage bytes → graceful empty page, not a crash."""
    pages = normalize_result(b"not a zip at all", markdown_fallback="#Recovered")
    assert len(pages) == 1
    assert "Recovered" in pages[0].markdown


def test_extract_from_zip() -> None:
    zip_bytes = _make_zip(middle_json={"pdf_info": []}, markdown="# MD")
    extracted = extract_from_zip(zip_bytes)
    assert "middle_json" in extracted
    assert extracted["middle_json"] == {"pdf_info": []}
    assert "markdown" in extracted
    assert "# MD" in extracted["markdown"]
