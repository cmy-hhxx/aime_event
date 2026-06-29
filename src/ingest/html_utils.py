import html
import re

import html2text

_converter = html2text.HTML2Text()
_converter.ignore_links = False
_converter.ignore_images = True
_converter.body_width = 0


def html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    text = _converter.handle(raw_html)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text
