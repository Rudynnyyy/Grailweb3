from __future__ import annotations

from io import BytesIO


def render_selection_png(*, title: str, rows: list[dict], top_n: int) -> bytes:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as e:
        raise RuntimeError(f"matplotlib_unavailable: {e}")

    n = max(0, min(int(top_n), len(rows)))
    picks = rows[:n]
    headers = ["Rank", "Symbol", "Market", "Close"]
    data: list[list[str]] = []
    for r in picks:
        rk = r.get("_rank")
        sym = str(r.get("symbol") or "")
        market = str(r.get("market") or "")
        close = r.get("close")
        close_s = ""
        try:
            if close is not None:
                close_s = f"{float(close):.6g}"
        except Exception:
            close_s = str(close) if close is not None else ""
        data.append([str(rk) if rk is not None else "", sym, market, close_s])

    fig_w = 10.0
    fig_h = max(2.2, 1.3 + 0.35 * max(1, n))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h))
    ax.axis("off")
    ax.set_title(str(title or ""), fontsize=14, fontweight="bold", loc="left", pad=8)

    table = ax.table(cellText=data, colLabels=headers, loc="center", cellLoc="left")
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
        if col in (0, 3):
            cell._loc = "right"
            cell.set_text_props(ha="right")

    buf = BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", pad_inches=0.2)
    plt.close(fig)
    return buf.getvalue()

