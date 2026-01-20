import io

from PyPDF2 import PdfWriter

from hello_world.s3_ingest import extract_pdf_metadata


def _make_blank_pdf_bytes(num_pages: int = 1) -> bytes:
    """
    Create a simple PDF in memory with blank pages using PyPDF2 only.
    Returns raw PDF bytes.
    """
    writer = PdfWriter()
    for _ in range(num_pages):
        writer.add_blank_page(width=612, height=792)  # 8.5x11 in points

    buffer = io.BytesIO()
    writer.write(buffer)
    return buffer.getvalue()


def test_extract_pdf_metadata_returns_page_count():
    pdf_bytes = _make_blank_pdf_bytes(1)

    page_count, preview = extract_pdf_metadata(pdf_bytes)

    assert page_count == 1
    assert isinstance(preview, str)


def test_extract_pdf_metadata_preview_is_capped_at_500_chars():
    pdf_bytes = _make_blank_pdf_bytes(2)

    page_count, preview = extract_pdf_metadata(pdf_bytes)

    assert page_count == 2
    assert len(preview) <= 500
