"""Utility helpers (text cleaning, etc.)."""

def clean_some_unicode_from_text(text: str) -> str:
    chars_to_remove = "\u061C"  # Arabic letter mark
    chars_to_remove += "\u200B\u200C\u200D"  # Zero-width space, non/ joiner
    chars_to_remove += "\u200E\u200F"  # LTR/RTL marks
    chars_to_remove += "\u202A\u202B\u202C\u202D\u202E"  # embeddings/overrides
    chars_to_remove += "\u2066\u2067\u2068\u2069"  # isolate controls
    chars_to_remove += "\uFEFF"  # zero-width no-break space
    return text.translate({ord(c): None for c in chars_to_remove})

