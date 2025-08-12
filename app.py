from __future__ import annotations

import time
import uuid
import hmac
import jwt
import hashlib
import threading
from typing import Any, Dict, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS

from api.api_key import load_api_keys


# ===================== Flask =====================
app = Flask(__name__)
CORS(app)


# ===================== Robust HTTP Session =====================
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=0.5,
        status_forcelist=(408, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "POST"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


HTTP = _make_session()
TIMEOUT = (3, 7)  # (connect, read) seconds


# ===================== Global State =====================
running: bool = False
position_state: str = "neutral"          # neutral | entered
trade_count: int = 0
profit_krw: float = 0.0
total_pnl: float = 0.0

logs: list[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}

_last_action_ts: float = 0.0
COOLDOWN_SEC: float = 5.0

# 진입 당시 김프 저장(청산 방향 판정용)
entry_kimp_value: float | None = None


def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 300:
        del logs[:100]


def floor_step(x: float, step: float) -> float:
    return (int(float(x) / step)) * step


# ===================== Price & FX =====================
def get_upbit_price() -> float:
    url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
    r = HTTP.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()[0]["trade_price"])


def get_binance_price() -> float:
    url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"
    r = HTTP.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()["price"])


def get_usdkrw() -> float:
    try:
        url = "https://www.google.com/finance/quote/USD-KRW"
        headers = {"User-Agent": "Mozilla/5.0"}
        text = HTTP.get(url, headers=headers, timeout=TIMEOUT).text
        s = text.find('data-last-price="')
        if s == -1:
            return 1300.0
        s += len('data-last-price="')
        e = text.find('"', s)
        return float(text[s:e].replace(",", ""))
    except Exception:
        return 1300.0


def calc_kimp() -> Tuple[float, float, float, float]:
    up = get_upbit_price()
    bp = get_binance_price()
    fx = get_usdkrw()
    k = ((up - bp * fx) / (bp * fx)) * 100.0
    return round(k, 2), up, bp, fx


# ===================== Balance =====================
def get_upbit_balance_real() -> Tuple[float, float]:
    keys = load_api_keys()
    access_key = keys.get("upbit_key", "")
    secret_key = keys.get("upbit_secret", "")

    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    headers = {"Authorization": f"Bearer {jwt_token}"}

    try:
        r = HTTP.get("https://api.upbit.com/v1/accounts", headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        data = r.json()
        krw = 0.0
        btc = 0.0
        for b in data:
            if b["currency"] == "KRW":
                krw = float(b["balance"])
            elif b["currency"] == "BTC":
                btc = float(b["balance"]) + float(b["locked"])
        return krw, btc
    except Exception as e:
        log(f"[UPBIT 잔고 오류] {e}")
        return 0.0, 0.0


def get_binance_available_usdt() -> float:
    """선물 지갑 가용 USDT"""
    keys = load_api_keys()
    api_key = keys.get("binance_key", "")
    api_secret = keys.get("binance_secret", "")

    ts = int(time.time() * 1000)
    params = {"timestamp": ts, "recvWindow": 5000}
    qs = urlencode(params)
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/balance?{qs}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}

    try:
        r = HTTP.get(url, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        for b in r.json():
            if b.get("asset") == "USDT":
                return float(b.get("availableBalance", 0.0))
        return 0.0
    except Exception as e:
        log(f"[BINANCE 잔고 오류] {e}")
        return 0.0


def get_binance_position_qty() -> float:
    keys = load_api_keys()
    api_key = keys.get("binance_key", "")
    api_secret = keys.get("binance_secret", "")

    ts = int(time.time() * 1000)
    qs = urlencode({"timestamp": ts, "recvWindow": 5000})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{qs}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}

    try:
        r = HTTP.get(url, headers=headers, timeout=TIMEOUT)
        r.raise_for_status()
        for p in r.json():
            if p.get("symbol") == "BTCUSDT":
                return float(p.get("positionAmt", 0.0))  # 숏이면 음수
        return 0.0
    except Exception as e:
        log(f"[BINANCE 포지션 오류] {e}")
        return 0.0


# ===================== Orders =====================
def _bn_signed_url(path: str, params: dict) -> Tuple[str, dict]:
    keys = load_api_keys()
    api_key = keys.get("binance_key", "")
    api_secret = keys.get("binance_secret", "")
    if "timestamp" not in params:
        params["timestamp"] = int(time.time() * 1000)
    if "recvWindow" not in params:
        params["recvWindow"] = 5000
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com{path}?{qs}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}
    return url, headers


def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        url, headers = _bn_signed_url("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        r = HTTP.post(url, headers=headers, timeout=TIMEOUT)
        log(f"[바이낸스 레버리지 설정] {symbol} x{leverage} → {r.text}")
        return r.status_code == 200
    except Exception as e:
        log(f"[레버리지 설정 오류] {e}")
        return False


def set_binance_isolated(symbol: str) -> bool:
    """격리(ISOLATED) 마진으로 설정(이미 격리면 성공 간주)"""
    try:
        url, headers = _bn_signed_url("/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"})
        r = HTTP.post(url, headers=headers, timeout=TIMEOUT)
        if r.status_code == 200:
            log(f"[마진모드] {symbol} 격리(ISOLATED) 설정 완료")
            return True
        else:
            log(f"[마진모드 응답] {r.text} (이미 격리일 수 있음)")
            return True
    except Exception as e:
        log(f"[마진모드 설정 오류] {e}")
        return False


def ensure_binance_margin_and_leverage(symbol: str, leverage: int) -> None:
    set_binance_isolated(symbol)
    for _ in range(3):
        set_binance_leverage(symbol, leverage)
        time.sleep(0.3)
        # 확인
        url, headers = _bn_signed_url("/fapi/v2/positionRisk", {})
        r = HTTP.get(url, headers=headers, timeout=TIMEOUT)
        try:
            for p in r.json():
                if p.get("symbol") == symbol:
                    cur = int(float(p.get("leverage", "0")))
                    log(f"[레버리지 확인] 현재={cur}, 목표={leverage}")
                    if cur == leverage:
                        return
        except Exception:
            pass
    log(f"[경고] 레버리지 {leverage} 설정 확인 실패")


def upbit_order(side: str, price_krw: float, volume_btc: float) -> bool:
    """
    Upbit 시장가 주문
    - buy : KRW 금액 시장가 (ord_type='price', price=정수 KRW)
    - sell: BTC 수량 시장가   (ord_type='market', volume=BTC 수량)
    """
    try:
        keys = load_api_keys()
        access_key = keys.get("upbit_key", "")
        secret_key = keys.get("upbit_secret", "")

        if side == "buy":
            query = {
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "price",
                "price": str(int(price_krw)),
            }
        else:
            query = {
                "market": "KRW-BTC",
                "side": "ask",
                "ord_type": "market",
                "volume": format(float(volume_btc), ".4f"),
            }

        m = hashlib.sha512()
        m.update(urlencode(query).encode())
        payload = {
            "access_key": access_key,
            "nonce": str(uuid.uuid4()),
            "query_hash": m.hexdigest(),
            "query_hash_alg": "SHA512",
        }
        jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
        headers = {"Authorization": f"Bearer {jwt_token}"}

        r = HTTP.post("https://api.upbit.com/v1/orders", params=query, headers=headers, timeout=TIMEOUT)
        log(f"[업비트 주문] {side.upper()} vol={query.get('volume','')} price={query.get('price','')} → {r.text}")
        return r.status_code == 201
    except Exception as e:
        log(f"[업비트 주문 오류] {e}")
        return False


def binance_order(side: str, quantity: float, reduce_only: bool = False) -> bool:
    """시장가 주문. reduce_only=True면 포지션 축소만 허용(청산 안전장치)."""
    try:
        qty = floor_step(float(quantity), 0.001)
        if qty <= 0:
            return True
        url, headers = _bn_signed_url(
            "/fapi/v1/order",
            {
                "symbol": "BTCUSDT",
                "side": "SELL" if side == "sell" else "BUY",
                "type": "MARKET",
                "quantity": format(qty, ".3f"),
                "reduceOnly": "true" if reduce_only else None,
            },
        )
        r = HTTP.post(url, headers=headers, timeout=TIMEOUT)
        log(f"[바이낸스 주문] {side.upper()} qty={format(qty,'.3f')} reduceOnly={reduce_only} → {r.text}")
        return r.status_code == 200
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False


def adjust_binance_size_to_target(target_size_usdt: float, ref_price: float, tol_usdt: float = 5.0) -> None:
    """목표 명목가(USDT)에 맞도록 0.001 BTC 단위로 보정."""
    def cur_size_usdt() -> float:
        qty = abs(get_binance_position_qty())
        return qty * ref_price

    for _ in range(4):
        now = cur_size_usdt()
        diff = target_size_usdt - now
        if abs(diff) <= tol_usdt:
            log(f"[사이즈 OK] 목표 {round(target_size_usdt,2)}USDT, 현재 {round(now,2)}USDT (±{tol_usdt})")
            return
        qty = floor_step(abs(diff) / ref_price, 0.001)
        if qty <= 0:
            return
        if diff > 0:
            binance_order("sell", qty, reduce_only=False)
            log(f"[보정] 모자람 {round(diff,2)}USDT → 추가 SELL {qty} BTC")
        else:
            binance_order("buy", qty, reduce_only=True)
            log(f"[보정] 초과 {round(-diff,2)}USDT → BUY(RO) {qty} BTC")
        time.sleep(0.35)


# ===================== Strategy =====================
def _cooldown() -> bool:
    global _last_action_ts
    now = time.time()
    if now - _last_action_ts < COOLDOWN_SEC:
        return True
    _last_action_ts = now
    return False


def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global running, position_state, trade_count, entry_info, entry_kimp_value

    target_kimp = float(cfg["target_kimp"])
    exit_kimp   = float(cfg["exit_kimp"])
    tolerance   = float(cfg["tolerance"])
    amount_krw  = float(cfg["amount_krw"])
    leverage    = int(float(cfg["leverage"]))

    ensure_binance_margin_and_leverage("BTCUSDT", leverage)

    entry_upbit_qty = 0.0

    while running:
        try:
            if _cooldown():
                time.sleep(0.3)
                continue

            kimp, up, bp, fx = calc_kimp()

            # -------- Entry --------
            if position_state == "neutral":
                # 방향성 + 허용오차 버퍼
                if target_kimp < 0:
                    near_ok = (kimp <= target_kimp + tolerance)
                else:
                    near_ok = (kimp >= target_kimp - tolerance)

                log(f"[체크] kimp={kimp}% target={target_kimp} tol={tolerance} near_ok={near_ok} state={position_state}")
                if not near_ok:
                    continue

                # 업비트 수량(현물 금액 기준)
                upbit_qty = round(amount_krw / up, 3)

                # 바이낸스 목표 사이즈/증거금
                target_size_usdt = amount_krw / fx
                need_margin_usdt = target_size_usdt / leverage
                avail_usdt = get_binance_available_usdt()
                log(f"[검사] 목표사이즈≈{round(target_size_usdt,2)}USDT, 필요증거금≈{round(need_margin_usdt,2)}USDT, 가용≈{round(avail_usdt,2)}USDT")

                if avail_usdt + 1e-6 < need_margin_usdt:
                    log("[진입 중단] 바이낸스 가용 USDT가 필요 증거금보다 적습니다.")
                    continue

                ok_upbit = upbit_order("buy", amount_krw, upbit_qty)
                if not ok_upbit:
                    log("[진입 실패] 업비트 매수 실패")
                    continue

                first_qty = floor_step(target_size_usdt / bp, 0.001)
                ok_binance = binance_order("sell", first_qty, reduce_only=False)
                if not ok_binance:
                    log("[진입 실패] 바이낸스 숏 실패 → 업비트 되돌림")
                    upbit_order("sell", 0.0, upbit_qty)
                    continue

                time.sleep(0.6)
                adjust_binance_size_to_target(target_size_usdt, ref_price=bp, tol_usdt=5.0)

                entry_upbit_qty = upbit_qty
                entry_info.update({
                    "upbit_qty": upbit_qty,
                    "binance_qty": floor_step(target_size_usdt / bp, 0.001)
                })
                position_state = "entered"
                entry_kimp_value = float(kimp)
                log(f"[진입 성공] 업비트 {upbit_qty}BTC(≈{int(amount_krw)}KRW) / "
                    f"바이낸스 숏 사이즈≈{round(target_size_usdt,2)}USDT (격리 x{leverage}, 필요증거금≈{round(need_margin_usdt,2)}USDT) "
                    f"@ 김프 {kimp}%")
                continue

            # -------- Exit --------
            if position_state == "entered":
                crossed = False
                if entry_kimp_value is not None:
                    crossed = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)

                if crossed:
                    ok_upbit = upbit_order("sell", 0.0, entry_upbit_qty)
                    pos_amt = get_binance_position_qty()
                    ok_binance = True
                    if pos_amt < -1e-6:
                        ok_binance = binance_order("buy", abs(pos_amt), reduce_only=True)

                    if ok_upbit and ok_binance:
                        trade_count += 1
                        position_state = "neutral"
                        entry_info.update({"upbit_qty": 0.0, "binance_qty": 0.0})
                        log(f"[청산 성공] 김프 {kimp}% (진입 {entry_kimp_value} → 청산선 {exit_kimp})")
                        entry_kimp_value = None
                    else:
                        if not ok_upbit and ok_binance:
                            log("[청산 일부실패] 업비트 실패 → 바이낸스 되돌림(숏 재진입 시도 필요)")
                        elif ok_upbit and not ok_binance:
                            log("[청산 일부실패] 바이낸스 실패 → 업비트 되돌림(매수 재진입 시도 필요)")
                continue

        except Exception as e:
            log(f"[전략 오류] {e}")

        time.sleep(0.5)


# ===================== Routes =====================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/current_kimp")
def current_kimp():
    k, up, bp, fx = calc_kimp()
    return jsonify({"kimp": k, "upbit_price": up, "binance_price": bp, "usdkrw": fx})


@app.route("/balance")
def balance():
    krw, btc = get_upbit_balance_real()
    usdt = get_binance_available_usdt()
    return jsonify({"real": {"krw": round(krw, 0), "btc_upbit": round(btc, 6), "usdt": round(usdt, 3)}})


@app.route("/status")
def status():
    return jsonify(
        {
            "running": running,
            "position_state": position_state,
            "trade_count": trade_count,
            "logs": logs,
            "entry_info": entry_info,
        }
    )


@app.route("/start", methods=["POST"])
def start():
    global running
    if not running:
        cfg_in = request.json or {}
        cfg = {
            "target_kimp": float(cfg_in.get("target_kimp", 0.0)),
            "exit_kimp": float(cfg_in.get("exit_kimp", 0.3)),
            "tolerance": float(cfg_in.get("tolerance", 0.1)),
            "amount_krw": float(cfg_in.get("amount_krw", 1_000_000)),
            "leverage": int(float(cfg_in.get("leverage", 3))),
        }
        running = True
        threading.Thread(target=run_strategy_thread, args=(cfg,), daemon=True).start()
        log(f"[전략 시작] cfg={cfg}")
    return jsonify({"status": "started"})


@app.route("/stop", methods=["POST"])
def stop():
    global running, entry_kimp_value
    running = False
    entry_kimp_value = None
    log("[전략 중지]")
    return jsonify({"status": "stopped"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
