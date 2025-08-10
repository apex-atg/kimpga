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


def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 300:
        del logs[:100]


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


def get_binance_balance_real() -> float:
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
                return round(float(b.get("balance", 0.0)), 3)
        return 0.0
    except Exception as e:
        log(f"[BINANCE 잔고 오류] {e}")
        return 0.0


# ===================== Orders =====================
def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        keys = load_api_keys()
        api_key = keys.get("binance_key", "")
        api_secret = keys.get("binance_secret", "")

        ts = int(time.time() * 1000)
        params = {"symbol": symbol, "leverage": leverage, "timestamp": ts, "recvWindow": 5000}
        qs = urlencode(params)
        sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"https://fapi.binance.com/fapi/v1/leverage?{qs}&signature={sig}"
        r = HTTP.post(url, headers={"X-MBX-APIKEY": api_key}, timeout=TIMEOUT)
        log(f"[바이낸스 레버리지 설정] {symbol} x{leverage} → {r.text}")
        return r.status_code == 200
    except Exception as e:
        log(f"[레버리지 설정 오류] {e}")
        return False


def upbit_order(side: str, price: float, volume: float) -> bool:
    keys = load_api_keys()
    access_key = keys.get("upbit_key", "")
    secret_key = keys.get("upbit_secret", "")

    if side == "buy":
        query = {"market": "KRW-BTC", "side": "bid", "ord_type": "price", "price": str(int(price))}
    else:
        query = {
            "market": "KRW-BTC",
            "side": "ask",
            "ord_type": "market",
            "volume": format(float(volume), ".3f"),
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
    log(f"[업비트 주문] {side.upper()} vol={format(float(volume),'.3f')} price={price} → {r.text}")
    return r.status_code == 201


def binance_order(side: str, price: float, volume: float) -> bool:
    try:
        keys = load_api_keys()
        api_key = keys.get("binance_key", "")
        api_secret = keys.get("binance_secret", "")

        ts = int(time.time() * 1000)
        params = {
            "symbol": "BTCUSDT",
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(float(volume), ".3f"),
            "timestamp": ts,
            "recvWindow": 5000,
        }
        qs = urlencode(params)
        sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"https://fapi.binance.com/fapi/v1/order?{qs}&signature={sig}"
        r = HTTP.post(url, headers={"X-MBX-APIKEY": api_key}, timeout=TIMEOUT)
        log(f"[바이낸스 주문] {side.upper()} vol={format(float(volume),'.3f')} @ {price} → {r.text}")
        return r.status_code == 200
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False


# ===================== Strategy =====================
def _cooldown() -> bool:
    global _last_action_ts
    now = time.time()
    if now - _last_action_ts < COOLDOWN_SEC:
        return True
    _last_action_ts = now
    return False


def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global running, position_state, trade_count, entry_info

    target_kimp = float(cfg["target_kimp"])
    exit_kimp = float(cfg["exit_kimp"])
    tolerance = float(cfg["tolerance"])
    amount_krw = float(cfg["amount_krw"])
    leverage = int(float(cfg["leverage"]))

    set_binance_leverage("BTCUSDT", leverage)

    entry_upbit_qty = 0.0
    entry_binance_qty = 0.0

    while running:
        try:
            if _cooldown():
                time.sleep(0.3)
                continue

            kimp, up, bp, _ = calc_kimp()

            # -------- Entry --------
            if position_state == "neutral" and abs(kimp - target_kimp) <= tolerance:
                upbit_qty = round(amount_krw / up, 3)
                binance_qty = round(upbit_qty / leverage, 3)

                ok_upbit = upbit_order("buy", amount_krw, upbit_qty)
                if not ok_upbit:
                    log("[진입 실패] 업비트 매수 실패")
                    continue

                ok_binance = binance_order("sell", bp, binance_qty)
                if not ok_binance:
                    log("[진입 실패] 바이낸스 숏 실패 → 업비트 되돌림")
                    upbit_order("sell", 0.0, upbit_qty)
                    continue

                entry_upbit_qty = upbit_qty
                entry_binance_qty = binance_qty
                entry_info.update({"upbit_qty": upbit_qty, "binance_qty": binance_qty})
                position_state = "entered"
                log(f"[진입 성공] 업비트 {upbit_qty} / 바이낸스 {binance_qty} (김프 {kimp}%)")
                continue

            # -------- Exit --------
            if position_state == "entered" and kimp >= exit_kimp:
                ok_upbit = upbit_order("sell", 0.0, entry_upbit_qty)
                ok_binance = binance_order("buy", bp, entry_binance_qty)

                if ok_upbit and ok_binance:
                    trade_count += 1
                    position_state = "neutral"
                    entry_info.update({"upbit_qty": 0.0, "binance_qty": 0.0})
                    log(f"[청산 성공] 업비트 {entry_upbit_qty} / 바이낸스 {entry_binance_qty} (김프 {kimp}%)")
                else:
                    if not ok_upbit and ok_binance:
                        log("[청산 일부실패] 업비트 실패 → 바이낸스 되돌림(숏 재진입)")
                        binance_order("sell", bp, entry_binance_qty)
                    elif ok_upbit and not ok_binance:
                        log("[청산 일부실패] 바이낸스 실패 → 업비트 되돌림(매수 재진입)")
                        upbit_order("buy", amount_krw, entry_upbit_qty)

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
    usdt = get_binance_balance_real()
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
        # 기본값 (지금 구조 유지)
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
    global running
    running = False
    log("[전략 중지]")
    return jsonify({"status": "stopped"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
