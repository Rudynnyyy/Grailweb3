from __future__ import annotations

import os
from io import BytesIO


def render_selection_png(*, title: str, rows: list[dict], top_n: int) -> bytes:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        raise RuntimeError(f"matplotlib_unavailable: {e}")

    def _setup_cjk_font():
        try:
            from matplotlib import font_manager

            env_font = str(os.environ.get("QC_CJK_FONT") or "").strip()
            if env_font and os.path.exists(env_font):
                try:
                    fp = font_manager.FontProperties(fname=env_font)
                    matplotlib.rcParams["axes.unicode_minus"] = False
                    return fp
                except Exception:
                    pass

            candidates = [
                ("Microsoft YaHei", [r"C:\Windows\Fonts\msyh.ttc", r"C:\Windows\Fonts\msyh.ttf"]),
                ("SimHei", [r"C:\Windows\Fonts\simhei.ttf"]),
                ("SimSun", [r"C:\Windows\Fonts\simsun.ttc"]),
                ("PingFang SC", []),
                ("Noto Sans CJK SC", []),
                ("WenQuanYi Zen Hei", []),
                ("Source Han Sans CN", []),
                ("Arial Unicode MS", []),
            ]
            for name, files in candidates:
                for p in files:
                    if p and os.path.exists(p):
                        try:
                            fp = font_manager.FontProperties(fname=p)
                            matplotlib.rcParams["axes.unicode_minus"] = False
                            return fp
                        except Exception:
                            continue
                try:
                    fp = font_manager.FontProperties(family=name)
                    path = font_manager.findfont(fp, fallback_to_default=False)
                    if path:
                        matplotlib.rcParams["font.sans-serif"] = [name]
                        matplotlib.rcParams["axes.unicode_minus"] = False
                        return fp
                except Exception:
                    continue

            try:
                import glob

                patterns = [
                    "/usr/share/fonts/**/NotoSansCJK*.ttc",
                    "/usr/share/fonts/**/NotoSansCJK*.otf",
                    "/usr/share/fonts/**/SourceHanSans*SC*.otf",
                    "/usr/share/fonts/**/wqy-zenhei*.ttc",
                    "/usr/local/share/fonts/**/NotoSansCJK*.ttc",
                    "/usr/local/share/fonts/**/NotoSansCJK*.otf",
                    os.path.expanduser("~/.fonts/**/NotoSansCJK*.ttc"),
                    os.path.expanduser("~/.fonts/**/NotoSansCJK*.otf"),
                ]
                for pat in patterns:
                    for p in glob.glob(pat, recursive=True):
                        if p and os.path.exists(p):
                            try:
                                fp = font_manager.FontProperties(fname=p)
                                matplotlib.rcParams["axes.unicode_minus"] = False
                                return fp
                            except Exception:
                                continue
            except Exception:
                pass
        except Exception:
            return None
        return None

    font_prop = _setup_cjk_font()

    def _strip_quote(symbol: str) -> str:
        s = str(symbol or "")
        if s.endswith("-USDT"):
            return s[:-5]
        if s.endswith("USDT"):
            return s[:-4]
        return s

    def _market_label(market: str) -> str:
        m = str(market or "").lower()
        if m == "spot":
            return "现货"
        if m == "swap":
            return "合约"
        return str(market or "")

    def _fmt_num(x) -> str:
        try:
            if x is None:
                return ""
            v = float(x)
            if v != v:
                return ""
            return f"{v:.6g}"
        except Exception:
            return ""

    def _prev_close(r: dict) -> float | None:
        try:
            series = (r or {}).get("series") or {}
            closes = series.get("close") if isinstance(series, dict) else None
            if not isinstance(closes, list) or len(closes) < 2:
                return None
            v = closes[-2]
            return float(v) if v is not None else None
        except Exception:
            return None

    def _pct_change(r: dict, prev_close: float | None) -> float | None:
        try:
            v = (r or {}).get("pct_change")
            if v is not None:
                x = float(v)
                if x == x:
                    return x
        except Exception:
            pass
        try:
            c1 = (r or {}).get("close")
            if prev_close is None or prev_close == 0:
                return None
            if c1 is None:
                return None
            x1 = float(c1)
            if x1 != x1:
                return None
            return (x1 / float(prev_close) - 1.0) * 100.0
        except Exception:
            return None

    n = max(0, min(int(top_n), len(rows)))
    picks = rows[:n]
    headers = ["排名", "币种", "市场", "上小时收盘", "收盘价", "涨跌幅(%)"]
    data: list[list[str]] = []
    for r in picks:
        rk = r.get("_rank")
        sym = _strip_quote(str(r.get("symbol") or ""))
        market = _market_label(str(r.get("market") or ""))
        close = r.get("close")
        prev = _prev_close(r)
        pct = _pct_change(r, prev)
        data.append(
            [
                str(rk) if rk is not None else "",
                sym,
                market,
                _fmt_num(prev),
                _fmt_num(close),
                _fmt_num(pct),
            ]
        )
    if not data:
        data = [["", "无命中", "", "", "", ""]]

    fig_w = 10.0
    fig_h = max(2.2, 0.9 + 0.35 * max(1, n))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")
    fig.suptitle("")
    ax.set_title("")

    table = ax.table(cellText=data, colLabels=headers, loc="center", cellLoc="center", colLoc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.35)

    for (row, col), cell in table.get_celld().items():
        cell.set_linewidth(0.5)
        if row == 0:
            cell.set_text_props(weight="bold", color="#111827")
            cell.set_facecolor("#E5E7EB")
        else:
            cell.set_facecolor("#FFFFFF" if row % 2 == 1 else "#F9FAFB")
        cell._loc = "center"
        cell.set_text_props(ha="center")
        if font_prop is not None:
            try:
                cell.get_text().set_fontproperties(font_prop)
            except Exception:
                pass

    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    return buf.getvalue()
