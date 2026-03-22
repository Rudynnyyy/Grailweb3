"""backtest/charts.py — 回测结果图表生成

与 wecom_report.py 完全相同的风格：
- 同一套 _setup_cjk_font() 字体逻辑
- 相同配色体系（#E5E7EB / #111827 / #2563EB 等）
- 相同 DPI=160, tight_layout, pad_inches=0.15
服务端生成 PNG bytes，前端用 <img src="..."> 展示，无需任何 JS 绘图库。
"""
from __future__ import annotations

import os
from io import BytesIO
from typing import Any

# ── 配色常量（与 wecom_report.py render_selection_png 完全一致）──
COLOR_HEADER_BG   = "#E5E7EB"
COLOR_HEADER_FG   = "#111827"
COLOR_ROW_ODD     = "#FFFFFF"
COLOR_ROW_EVEN    = "#F9FAFB"
COLOR_LINE_EQUITY = "#2563EB"   # 净值曲线（蓝）
COLOR_LINE_BENCH  = "#9CA3AF"   # 基准线（灰）
COLOR_BAR_SIGNAL  = "#6B7280"   # 信号柱（灰）
COLOR_BAR_POS     = "#10B981"   # 正收益（绿）
COLOR_BAR_NEG     = "#EF4444"   # 负收益（红）
COLOR_FILL_EQUITY = "#DBEAFE"   # 净值曲线填充（浅蓝）
COLOR_DRAWDOWN    = "#FEE2E2"   # 回撤区域填充（浅红）


def _setup_matplotlib():
    """初始化 matplotlib Agg 后端"""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        return matplotlib, plt
    except Exception as e:
        raise RuntimeError(f"matplotlib 不可用: {e}")


def _setup_cjk_font():
    """与 wecom_report.py 完全相同的 CJK 字体查找逻辑"""
    try:
        import matplotlib
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

        # Linux 系统字体路径
        try:
            import glob as _glob
            for pat in [
                "/usr/share/fonts/**/NotoSansCJK*.ttc",
                "/usr/share/fonts/**/NotoSansCJK*.otf",
                "/usr/share/fonts/**/wqy-zenhei*.ttc",
                "/usr/local/share/fonts/**/NotoSansCJK*.ttc",
                os.path.expanduser("~/.fonts/**/NotoSansCJK*.ttc"),
            ]:
                for p in _glob.glob(pat, recursive=True):
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
        pass
    return None


def _strip_quote(symbol: str) -> str:
    s = str(symbol or "")
    if s.endswith("-USDT"):
        return s[:-5]
    if s.endswith("USDT"):
        return s[:-4]
    return s


def render_equity_png(
    *,
    equity_curve: list[dict],
    stats: dict,
    title: str = "回测净值曲线",
) -> bytes:
    """
    生成回测净值曲线 PNG（三子图：净值 + 回撤 + 持仓数）。
    风格与 wecom_report.render_selection_png 完全一致。
    """
    matplotlib, plt = _setup_matplotlib()
    font_prop = _setup_cjk_font()

    if not equity_curve:
        raise ValueError("equity_curve 为空")

    dts    = [e["dt"][:10] for e in equity_curve]
    equity = [float(e["equity"]) for e in equity_curve]
    n_held = [int(e["n_held"]) for e in equity_curve]

    # 计算回撤序列
    peak = 1.0
    drawdown = []
    for eq in equity:
        if eq > peak:
            peak = eq
        drawdown.append((peak - eq) / peak if peak > 0 else 0.0)

    fig, axes = plt.subplots(
        3, 1,
        figsize=(12, 8),
        gridspec_kw={"height_ratios": [3, 1, 1]},
        sharex=True,
    )
    fig.patch.set_facecolor("#FFFFFF")

    xs = range(len(equity))

    # ── 子图1：净值曲线 ──
    ax1 = axes[0]
    ax1.set_facecolor("#FFFFFF")
    ax1.plot(xs, equity, color=COLOR_LINE_EQUITY, lw=1.5, zorder=3)
    ax1.fill_between(
        xs, 1.0, equity,
        where=[e >= 1.0 for e in equity],
        color=COLOR_FILL_EQUITY, alpha=0.4, zorder=2,
    )
    ax1.axhline(1.0, color=COLOR_LINE_BENCH, lw=0.8, ls="--", zorder=1)

    tr_pct  = stats.get("total_return", 0) * 100
    ann_pct = stats.get("annualized_return", 0) * 100
    sharpe  = stats.get("sharpe_ratio", 0)
    ann_str = f"总收益 {tr_pct:+.1f}%  年化 {ann_pct:+.1f}%  夏普 {sharpe:.2f}"

    fp_kw = {"fontproperties": font_prop} if font_prop else {}
    ax1.set_title(f"{title}\n{ann_str}", fontsize=11, color=COLOR_HEADER_FG, pad=8, **fp_kw)
    ax1.set_ylabel("净值", fontsize=9, **fp_kw)
    ax1.tick_params(labelsize=8)
    ax1.grid(axis="y", color="#E5E7EB", lw=0.5)
    ax1.spines[["top", "right"]].set_visible(False)

    # ── 子图2：回撤 ──
    ax2 = axes[1]
    ax2.set_facecolor("#FFFFFF")
    ax2.fill_between(xs, 0, [-d for d in drawdown], color=COLOR_DRAWDOWN, alpha=0.8)
    ax2.set_ylabel("回撤", fontsize=9, **fp_kw)
    ax2.tick_params(labelsize=8)
    ax2.grid(axis="y", color="#E5E7EB", lw=0.5)
    ax2.spines[["top", "right"]].set_visible(False)
    ax2.yaxis.set_major_formatter(
        plt.FuncFormatter(lambda v, _: f"{abs(v) * 100:.0f}%")
    )

    # ── 子图3：每期持仓数 ──
    ax3 = axes[2]
    ax3.set_facecolor("#FFFFFF")
    ax3.bar(xs, n_held, color=COLOR_BAR_SIGNAL, alpha=0.7, width=1.0)
    ax3.set_ylabel("持仓数", fontsize=9, **fp_kw)
    ax3.tick_params(labelsize=8)
    ax3.grid(axis="y", color="#E5E7EB", lw=0.5)
    ax3.spines[["top", "right"]].set_visible(False)

    # X轴刻度：均匀取~8个日期标签
    n = len(dts)
    step = max(1, n // 8)
    tick_pos = list(range(0, n, step))
    ax3.set_xticks(tick_pos)
    ax3.set_xticklabels(
        [dts[i] for i in tick_pos],
        rotation=30, ha="right", fontsize=7,
    )
    if font_prop:
        for lbl in ax3.get_xticklabels():
            lbl.set_fontproperties(font_prop)

    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    return buf.getvalue()


def render_topsymbols_png(
    *,
    top_symbols: list[dict],
    title: str = "Top 入选频次",
) -> bytes:
    """
    生成 Top 符号频次横向条形图。
    配色与 wecom_report.render_selection_png 的表格配色完全一致。
    """
    matplotlib, plt = _setup_matplotlib()
    font_prop = _setup_cjk_font()

    items = (top_symbols or [])[:20]
    if not items:
        raise ValueError("top_symbols 为空")

    labels = [_strip_quote(x["symbol"]) for x in items][::-1]
    values = [int(x["hits"]) for x in items][::-1]

    fig_h = max(3.0, len(items) * 0.38)
    fig, ax = plt.subplots(figsize=(8, fig_h))
    fig.patch.set_facecolor("#FFFFFF")
    ax.set_facecolor("#FFFFFF")

    ax.barh(range(len(labels)), values,
            color=COLOR_LINE_EQUITY, alpha=0.85, height=0.65)
    ax.set_yticks(range(len(labels)))
    ax.set_yticklabels(labels, fontsize=9)
    if font_prop:
        for lbl in ax.get_yticklabels():
            lbl.set_fontproperties(font_prop)

    fp_kw = {"fontproperties": font_prop} if font_prop else {}
    ax.set_xlabel("入选次数", fontsize=9, **fp_kw)
    ax.set_title(title, fontsize=11, color=COLOR_HEADER_FG, pad=8, **fp_kw)
    ax.grid(axis="x", color="#E5E7EB", lw=0.5)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(labelsize=8)

    fig.tight_layout()
    buf = BytesIO()
    fig.savefig(buf, format="png", dpi=160, bbox_inches="tight", pad_inches=0.15)
    plt.close(fig)
    return buf.getvalue()
