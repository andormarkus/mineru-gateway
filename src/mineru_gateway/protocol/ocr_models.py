"""Mistral-compatible OCR request/response Pydantic models.

These match the Mistral ``/v1/ocr`` API shape so LiteLLM can point its OCR route at the gateway unchanged.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class OCRDocument(BaseModel):
    """The document to OCR. ``type`` selects the input mode."""

    type: str = Field(description="'document_url', 'image_url', or 'file' (base64 inline).")
    document_url: str | None = None
    image_url: str | None = None
    file: str | None = Field(default=None, description="Base64-encoded file content (type='file').")
    file_name: str | None = None


class OCRRequest(BaseModel):
    """POST /v1/ocr request body (Mistral-compatible)."""

    model: str = Field(default="mineru", description="Model identifier.")
    document: OCRDocument
    language: list[str] | None = None
    # Gateway passthroughs to MinerU parse options:
    backend: str = "hybrid-engine"
    effort: str = "medium"
    parse_method: str = "auto"


class OCRPageImage(BaseModel):
    """An image extracted from a page (optional, for include_image_base64)."""

    id: str
    image_base64: str | None = None
    top_left_x: int | None = None
    top_left_y: int | None = None
    bottom_right_x: int | None = None
    bottom_right_y: int | None = None
    image_annotation: str | None = None


class OCRPage(BaseModel):
    """A single page in the normalized Mistral OCR response."""

    index: int
    markdown: str
    images: list[OCRPageImage] = Field(default_factory=list)
    dimensions: dict[str, int] | None = None
