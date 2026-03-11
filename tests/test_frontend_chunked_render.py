from __future__ import annotations

import sys
import unittest
from pathlib import Path


repo_root = Path(__file__).resolve().parents[1]
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))


class FrontendChunkedRenderTests(unittest.TestCase):
    def test_app_js_has_chunked_render(self) -> None:
        p = repo_root / "apps" / "crypto_screener" / "web" / "app.js"
        txt = p.read_text(encoding="utf-8", errors="ignore")
        self.assertIn("function renderTableChunked", txt)


if __name__ == "__main__":
    unittest.main()

