from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Token:
    t: str
    v: Any


def tokenize_expr(src: str) -> list[Token]:
    s = str(src or "")
    out: list[Token] = []
    i = 0

    def is_digit(c: str) -> bool:
        return "0" <= c <= "9"

    def is_alpha(c: str) -> bool:
        return ("a" <= c <= "z") or ("A" <= c <= "Z") or c == "_"

    while i < len(s):
        c = s[i]
        if c in (" ", "\t", "\n", "\r"):
            i += 1
            continue
        if c in (">", "<", "=", "!"):
            n = s[i + 1] if i + 1 < len(s) else ""
            if n == "=":
                out.append(Token(f"{c}=", f"{c}="))
                i += 2
                continue
            if c == "=":
                raise ValueError("不支持单独的 =")
            out.append(Token(c, c))
            i += 1
            continue
        if is_digit(c) or (c == "." and is_digit(s[i + 1] if i + 1 < len(s) else "")):
            j = i + 1
            while j < len(s) and (is_digit(s[j]) or s[j] == "."):
                j += 1
            out.append(Token("num", float(s[i:j])))
            i = j
            continue
        if is_alpha(c):
            j = i + 1
            while j < len(s) and (is_alpha(s[j]) or is_digit(s[j])):
                j += 1
            out.append(Token("id", s[i:j]))
            i = j
            continue
        if c in "+-*/(),.":
            out.append(Token(c, c))
            i += 1
            continue
        raise ValueError(f"不支持字符：{c}")
    return out


def parse_expr_tokens(tokens: list[Token]) -> dict:
    pos = 0

    def peek() -> Token | None:
        return tokens[pos] if pos < len(tokens) else None

    def take(t: str | None = None) -> Token:
        nonlocal pos
        cur = peek()
        if cur is None or (t is not None and cur.t != t):
            raise ValueError("表达式解析失败")
        pos += 1
        return cur

    def parse_atom() -> dict:
        cur = peek()
        if cur is None:
            raise ValueError("表达式为空")
        if cur.t == "num":
            take("num")
            return {"k": "num", "v": float(cur.v)}
        if cur.t == "id":
            take("id")
            ident = str(cur.v)
            if peek() is not None and peek().t == "(":
                take("(")
                args: list[dict] = []
                if peek() is not None and peek().t != ")":
                    args.append(parse_add_sub())
                    while peek() is not None and peek().t == ",":
                        take(",")
                        args.append(parse_add_sub())
                take(")")
                return {"k": "call", "name": ident, "args": args}
            return {"k": "id", "name": ident}
        if cur.t == "(":
            take("(")
            e = parse_add_sub()
            take(")")
            return e
        raise ValueError("表达式解析失败")

    def parse_postfix() -> dict:
        node = parse_atom()
        while peek() is not None and peek().t == ".":
            take(".")
            m = str(take("id").v)
            if peek() is not None and peek().t == "(":
                take("(")
                args: list[dict] = [node]
                if peek() is not None and peek().t != ")":
                    args.append(parse_add_sub())
                    while peek() is not None and peek().t == ",":
                        take(",")
                        args.append(parse_add_sub())
                take(")")
                node = {"k": "call", "name": m, "args": args}
            else:
                node = {"k": "member", "obj": node, "prop": m}
        return node

    def parse_unary() -> dict:
        cur = peek()
        if cur is not None and cur.t in ("+", "-"):
            op = take().t
            return {"k": "unary", "op": op, "x": parse_unary()}
        return parse_postfix()

    def parse_mul_div() -> dict:
        node = parse_unary()
        while peek() is not None and peek().t in ("*", "/"):
            op = take().t
            node = {"k": "bin", "op": op, "a": node, "b": parse_unary()}
        return node

    def parse_add_sub() -> dict:
        node = parse_mul_div()
        while peek() is not None and peek().t in ("+", "-"):
            op = take().t
            node = {"k": "bin", "op": op, "a": node, "b": parse_mul_div()}
        return node

    def parse_cmp() -> dict:
        node = parse_add_sub()
        while peek() is not None and peek().t in (">", "<", ">=", "<=", "==", "!="):
            op = take().t
            node = {"k": "cmp", "op": op, "a": node, "b": parse_add_sub()}
        return node

    ast = parse_cmp()
    if pos != len(tokens):
        raise ValueError("表达式解析失败")
    return ast


@dataclass(frozen=True)
class Val:
    t: str
    v: Any


def _to_float(x: Any) -> float | None:
    try:
        y = float(x)
        if y != y:
            return None
        return y
    except Exception:
        return None


def _last_finite(arr) -> float | None:
    try:
        import numpy as np
    except Exception:
        np = None
    if np is not None and hasattr(arr, "shape"):
        xs = arr
        for i in range(int(xs.shape[0]) - 1, -1, -1):
            v = float(xs[i])
            if v == v:
                return v
        return None
    xs = list(arr or [])
    for i in range(len(xs) - 1, -1, -1):
        v = _to_float(xs[i])
        if v is not None:
            return v
    return None


def _as_series(v: Val, *, n_hint: int) -> Any:
    try:
        import numpy as np
    except Exception:
        np = None
    if v.t == "series":
        return v.v
    if v.t != "scalar" or n_hint <= 0:
        return np.array([], dtype="float64") if np is not None else []
    if np is not None:
        x = float(v.v) if v.v is not None else float("nan")
        return np.full((n_hint,), x, dtype="float64")
    return [v.v for _ in range(n_hint)]


def _scalar_from(v: Val) -> float | None:
    if v.t == "scalar":
        return v.v
    if v.t == "series":
        return _last_finite(v.v)
    return None


def _v_scalar(x: Any) -> Val:
    return Val("scalar", _to_float(x))


def _v_series(arr: Any) -> Val:
    return Val("series", arr)


def _is_series(v: Val) -> bool:
    return v.t == "series"


def _rolling_std_last(series, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    try:
        import numpy as np
    except Exception:
        np = None
    if np is None:
        xs = [_to_float(x) for x in list(series or [])]
        if len(xs) < w:
            return None
        tail = xs[-w:]
        if any(v is None for v in tail):
            return None
        m = sum(tail) / w
        var0 = sum((x - m) * (x - m) for x in tail) / w
        return var0 ** 0.5
    xs = np.asarray(series, dtype="float64")
    if xs.shape[0] < w:
        return None
    tail = xs[-w:]
    if np.isnan(tail).any():
        return None
    m = float(tail.mean())
    v = float(((tail - m) ** 2).mean())
    return v ** 0.5


def _sma_last(series, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    try:
        import numpy as np
    except Exception:
        np = None
    if np is None:
        xs = [_to_float(x) for x in list(series or [])]
        if len(xs) < w:
            return None
        tail = xs[-w:]
        if any(v is None for v in tail):
            return None
        return sum(tail) / w
    xs = np.asarray(series, dtype="float64")
    if xs.shape[0] < w:
        return None
    tail = xs[-w:]
    if np.isnan(tail).any():
        return None
    return float(tail.mean())


def _ema_last(series, window: int) -> float | None:
    w = int(window)
    if w <= 0:
        return None
    try:
        import numpy as np
    except Exception:
        np = None
    if np is None:
        xs = [_to_float(x) for x in list(series or [])]
        xs = [v for v in xs if v is not None]
        if len(xs) < w:
            return None
        alpha = 2.0 / (w + 1.0)
        ema = sum(xs[:w]) / w
        for x in xs[w:]:
            ema = alpha * x + (1.0 - alpha) * ema
        return float(ema)
    xs = np.asarray(series, dtype="float64")
    xs = xs[~np.isnan(xs)]
    if xs.shape[0] < w:
        return None
    alpha = 2.0 / (w + 1.0)
    ema = float(xs[:w].mean())
    for x in xs[w:]:
        ema = alpha * float(x) + (1.0 - alpha) * ema
    return float(ema)


def _rsi_last(series, period: int) -> float | None:
    p = int(period)
    if p <= 0:
        return None
    try:
        import numpy as np
    except Exception:
        np = None
    if np is None:
        xs = [_to_float(x) for x in list(series or [])]
        xs = [v for v in xs if v is not None]
        if len(xs) < p + 1:
            return None
        ys = xs[-(p + 120) :] if len(xs) > p + 120 else xs
        gains = []
        losses = []
        for i in range(1, len(ys)):
            d = ys[i] - ys[i - 1]
            gains.append(d if d > 0 else 0.0)
            losses.append(-d if d < 0 else 0.0)
        if len(gains) < p:
            return None
        avg_gain = sum(gains[:p]) / p
        avg_loss = sum(losses[:p]) / p
        for i in range(p, len(gains)):
            avg_gain = (avg_gain * (p - 1) + gains[i]) / p
            avg_loss = (avg_loss * (p - 1) + losses[i]) / p
        if avg_loss == 0 and avg_gain == 0:
            return 0.0
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return float(100 - (100 / (1 + rs)))
    xs = np.asarray(series, dtype="float64")
    xs = xs[~np.isnan(xs)]
    if xs.shape[0] < p + 1:
        return None
    ys = xs[-(p + 120) :] if xs.shape[0] > p + 120 else xs
    ds = np.diff(ys)
    gains = np.where(ds > 0, ds, 0.0)
    losses = np.where(ds < 0, -ds, 0.0)
    if gains.shape[0] < p:
        return None
    avg_gain = float(gains[:p].mean())
    avg_loss = float(losses[:p].mean())
    for i in range(p, gains.shape[0]):
        avg_gain = (avg_gain * (p - 1) + float(gains[i])) / p
        avg_loss = (avg_loss * (p - 1) + float(losses[i])) / p
    if avg_loss == 0 and avg_gain == 0:
        return 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return float(100 - (100 / (1 + rs)))


def _rolling_corr_last(x, y, window: int) -> float | None:
    w = int(window)
    if w <= 1:
        return None
    try:
        import numpy as np
    except Exception:
        np = None
    if np is None:
        xs = [_to_float(v) for v in list(x or [])]
        ys = [_to_float(v) for v in list(y or [])]
        n = min(len(xs), len(ys))
        if n < w:
            return None
        ax = xs[-w:]
        ay = ys[-w:]
        if any(v is None for v in ax) or any(v is None for v in ay):
            return None
        mx = sum(ax) / w
        my = sum(ay) / w
        cov = 0.0
        vx = 0.0
        vy = 0.0
        for i in range(w):
            dx = ax[i] - mx
            dy = ay[i] - my
            cov += dx * dy
            vx += dx * dx
            vy += dy * dy
        if vx == 0 or vy == 0:
            return None
        return cov / ((vx * vy) ** 0.5)
    ax = np.asarray(x, dtype="float64")
    ay = np.asarray(y, dtype="float64")
    n = min(ax.shape[0], ay.shape[0])
    if n < w:
        return None
    xs = ax[n - w : n]
    ys = ay[n - w : n]
    if np.isnan(xs).any() or np.isnan(ys).any():
        return None
    mx = float(xs.mean())
    my = float(ys.mean())
    dx = xs - mx
    dy = ys - my
    cov = float((dx * dy).sum())
    vx = float((dx * dx).sum())
    vy = float((dy * dy).sum())
    if vx == 0 or vy == 0:
        return None
    return cov / ((vx * vy) ** 0.5)


def eval_ast(node: dict, *, series: dict[str, Any], latest: dict[str, Any]) -> Val:
    k = node.get("k")
    if k == "num":
        return _v_scalar(node.get("v"))
    if k == "id":
        name = str(node.get("name") or "").lower()
        if name in ("open", "high", "low", "close", "volume", "quote_volume"):
            arr = series.get(name)
            return _v_series(arr if arr is not None else [])
        if name == "quotevolume":
            arr = series.get("quote_volume")
            return _v_series(arr if arr is not None else [])
        if name == "eps":
            return _v_scalar(1e-12)
        if name in latest:
            return _v_scalar(latest.get(name))
        return _v_scalar(None)
    if k == "member":
        return _v_scalar(None)
    if k == "unary":
        x = eval_ast(node["x"], series=series, latest=latest)
        op = str(node.get("op") or "")
        if _is_series(x):
            try:
                import numpy as np
            except Exception:
                np = None
            if np is not None:
                xs = np.asarray(x.v, dtype="float64")
                if op == "-":
                    return _v_series(-xs)
                return _v_series(xs)
            out = []
            for v in list(x.v or []):
                vv = _to_float(v)
                out.append((-vv if op == "-" else vv) if vv is not None else None)
            return _v_series(out)
        v0 = _scalar_from(x)
        if v0 is None:
            return _v_scalar(None)
        return _v_scalar(-v0 if op == "-" else v0)
    if k in ("bin", "cmp"):
        a = eval_ast(node["a"], series=series, latest=latest)
        b = eval_ast(node["b"], series=series, latest=latest)
        a_is_s = _is_series(a)
        b_is_s = _is_series(b)
        n = 0
        if a_is_s:
            n = len(a.v)
        elif b_is_s:
            n = len(b.v)
        if n <= 0:
            av = _scalar_from(a)
            bv = _scalar_from(b)
            if av is None or bv is None:
                return _v_scalar(None)
            op = str(node.get("op") or "")
            if k == "bin":
                if op == "+":
                    return _v_scalar(av + bv)
                if op == "-":
                    return _v_scalar(av - bv)
                if op == "*":
                    return _v_scalar(av * bv)
                if op == "/":
                    return _v_scalar(None if bv == 0 else av / bv)
                return _v_scalar(None)
            if op == ">":
                return _v_scalar(1.0 if av > bv else 0.0)
            if op == ">=":
                return _v_scalar(1.0 if av >= bv else 0.0)
            if op == "<":
                return _v_scalar(1.0 if av < bv else 0.0)
            if op == "<=":
                return _v_scalar(1.0 if av <= bv else 0.0)
            if op == "==":
                return _v_scalar(1.0 if av == bv else 0.0)
            if op == "!=":
                return _v_scalar(1.0 if av != bv else 0.0)
            return _v_scalar(None)
        as0 = _as_series(a, n_hint=n)
        bs0 = _as_series(b, n_hint=n)
        op = str(node.get("op") or "")
        try:
            import numpy as np
        except Exception:
            np = None
        if np is not None:
            x = np.asarray(as0, dtype="float64")
            y = np.asarray(bs0, dtype="float64")
            if k == "bin":
                if op == "+":
                    return _v_series(x + y)
                if op == "-":
                    return _v_series(x - y)
                if op == "*":
                    return _v_series(x * y)
                if op == "/":
                    with np.errstate(divide="ignore", invalid="ignore"):
                        z = x / y
                        z[y == 0] = np.nan
                    return _v_series(z)
                return _v_series(np.full((n,), np.nan, dtype="float64"))
            if op == ">":
                return _v_series((x > y).astype("float64"))
            if op == ">=":
                return _v_series((x >= y).astype("float64"))
            if op == "<":
                return _v_series((x < y).astype("float64"))
            if op == "<=":
                return _v_series((x <= y).astype("float64"))
            if op == "==":
                return _v_series((x == y).astype("float64"))
            if op == "!=":
                return _v_series((x != y).astype("float64"))
            return _v_series(np.full((n,), np.nan, dtype="float64"))
        out = []
        xs = list(as0)
        ys = list(bs0)
        for i in range(n):
            xv = _to_float(xs[i]) if i < len(xs) else None
            yv = _to_float(ys[i]) if i < len(ys) else None
            if xv is None or yv is None:
                out.append(None)
                continue
            if k == "bin":
                if op == "+":
                    out.append(xv + yv)
                elif op == "-":
                    out.append(xv - yv)
                elif op == "*":
                    out.append(xv * yv)
                elif op == "/":
                    out.append(None if yv == 0 else xv / yv)
                else:
                    out.append(None)
            else:
                if op == ">":
                    out.append(1.0 if xv > yv else 0.0)
                elif op == ">=":
                    out.append(1.0 if xv >= yv else 0.0)
                elif op == "<":
                    out.append(1.0 if xv < yv else 0.0)
                elif op == "<=":
                    out.append(1.0 if xv <= yv else 0.0)
                elif op == "==":
                    out.append(1.0 if xv == yv else 0.0)
                elif op == "!=":
                    out.append(1.0 if xv != yv else 0.0)
                else:
                    out.append(None)
        return _v_series(out)
    if k == "call":
        name = str(node.get("name") or "").lower()
        args = node.get("args") or []

        def ensure_series(n0) -> Any:
            v0 = eval_ast(n0, series=series, latest=latest)
            return v0.v if v0.t == "series" else []

        def ensure_scalar(n0) -> float | None:
            return _scalar_from(eval_ast(n0, series=series, latest=latest))

        if name == "abs":
            x = eval_ast(args[0], series=series, latest=latest) if args else _v_scalar(None)
            if _is_series(x):
                try:
                    import numpy as np
                except Exception:
                    np = None
                if np is not None:
                    return _v_series(np.abs(np.asarray(x.v, dtype="float64")))
                return _v_series([abs(float(v)) if _to_float(v) is not None else None for v in list(x.v or [])])
            v0 = _scalar_from(x)
            return _v_scalar(abs(v0) if v0 is not None else None)

        if name == "shift":
            if not args:
                return _v_scalar(None)
            if len(args) >= 2:
                s = ensure_series(args[0])
                n = ensure_scalar(args[1])
            else:
                s0 = series.get("close")
                s = s0 if s0 is not None else []
                n = ensure_scalar(args[0])
            if n is None:
                return _v_scalar(None)
            k0 = int(n)
            idx = len(s) - 1 - k0
            if idx < 0 or idx >= len(s):
                return _v_scalar(None)
            try:
                return _v_scalar(s[idx])
            except Exception:
                return _v_scalar(None)

        if name in ("ma", "sma", "ema", "mean", "std"):
            if not args:
                return _v_scalar(None)
            if len(args) >= 2:
                s = ensure_series(args[0])
                w = ensure_scalar(args[1])
            else:
                s0 = series.get("close")
                s = s0 if s0 is not None else []
                w = ensure_scalar(args[0])
            if w is None:
                return _v_scalar(None)
            win = int(w)
            if name == "std":
                return _v_scalar(_rolling_std_last(s, win))
            if name == "ema":
                return _v_scalar(_ema_last(s, win))
            return _v_scalar(_sma_last(s, win))

        if name == "rsi":
            if not args:
                return _v_scalar(None)
            if len(args) >= 2:
                s = ensure_series(args[0])
                p = ensure_scalar(args[1])
            else:
                s0 = series.get("close")
                s = s0 if s0 is not None else []
                p = ensure_scalar(args[0])
            if p is None:
                return _v_scalar(None)
            return _v_scalar(_rsi_last(s, int(p)))

        if name == "corr":
            if len(args) >= 3:
                x = ensure_series(args[0])
                y = ensure_series(args[1])
                w = ensure_scalar(args[2])
                if w is None:
                    return _v_scalar(None)
                return _v_scalar(_rolling_corr_last(x, y, int(w)))
            if len(args) == 2:
                a0 = args[0]
                b0 = args[1]
                if isinstance(a0, dict) and str(a0.get("k") or "") == "call" and str(a0.get("name") or "").lower() == "rolling":
                    rargs = a0.get("args") or []
                    if len(rargs) >= 2:
                        x = ensure_series(rargs[0])
                        w = ensure_scalar(rargs[1])
                        y = ensure_series(b0)
                        if w is None:
                            return _v_scalar(None)
                        return _v_scalar(_rolling_corr_last(x, y, int(w)))
            return _v_scalar(None)

        return _v_scalar(None)

    return _v_scalar(None)


def eval_expression_scalar(*, expr: str, series: dict[str, Any], latest: dict[str, Any]) -> float | None:
    tokens = tokenize_expr(expr)
    ast = parse_expr_tokens(tokens)
    v = eval_ast(ast, series=series, latest=latest)
    return _scalar_from(v)
