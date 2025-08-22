from __future__ import annotations

import os
import time
import uuid
import hmac
import jwt
import hashlib
import threading
from typing import Any, Dict, Tuple, Optional, List

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

profit_krw: float = 0.0                  # ★ 누적 실현손익 (KRW)
total_pnl: float = 0.0                   # (보존 필드)
fees_upbit_krw_cum: float = 0.0          # ★ 업비트 누적 수수료 (KRW)
fees_binance_usdt_cum: float = 0.0       # ★ 바이낸스 누적 수수료 (USDT)
fees_binance_krw_cum: float = 0.0        # ★ 바이낸스 누적 수수료 (KRW) ← 추가

logs: list[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}

_last_action_ts: float = 0.0
COOLDOWN_SEC: float = 5.0

last_error: Optional[str] = None
entry_kimp_value: Optional[float] = None  # ★ 진입 시 김프 저장 (청산 교차판정용)

# ★ 한 사이클의 체결/비용 기록 (PnL 계산용)
current_cycle: Dict[str, Any] = {
    "upbit_buy_uuid": None,
    "upbit_sell_uuid": None,
    "upbit_buy_krw": 0.0,
    "upbit_sell_krw": 0.0,
    "upbit_fee_krw": 0.0,

    "binance_sell_id": None,   # 숏 진입
    "binance_buy_id": None,    # 숏 청산
    "binance_entry_qty": 0.0,
    "binance_entry_avg": 0.0,
    "binance_exit_qty": 0.0,
    "binance_exit_avg": 0.0,
    "binance_fee_usdt": 0.0,
}


def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 300:
        del logs[:100]


# ===================== Binance base / time sync =====================
BINANCE_TESTNET = str(os.getenv("BINANCE_TESTNET", "false")).lower() in ("1", "true", "yes")
BASE_FAPI = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"
RECV_WINDOW = 5000  # ms
_MAX_TS_RETRY = 1   # -1021 자동 복구 재시도 1회

_bn_time_offset_ms = 0  # serverTime - local

def _bn_headers() -> dict:
    k = load_api_keys()
    return {
        "X-MBX-APIKEY": k.get("binance_key", ""),
        "Content-Type": "application/x-www-form-urlencoded",
    }

def _bn_now_ms() -> int:
    return int(time.time() * 1000) + _bn_time_offset_ms

def _bn_sign(params: Dict[str, Any]) -> str:
    k = load_api_keys()
    sec = k.get("binance_secret", "")
    qs = urlencode(params, doseq=True)
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def _is_ts_error(txt: str) -> bool:
    t = (txt or "").lower()
    return "-1021" in t or ("timestamp" in t and ("ahead" in t or "server time" in t or "recvwindow" in t))

def sync_binance_time() -> None:
    global _bn_time_offset_ms
    try:
        r = HTTP.get(f"{BASE_FAPI}/fapi/v1/time", timeout=TIMEOUT)
        r.raise_for_status()
        st = int(r.json()["serverTime"])
        _bn_time_offset_ms = st - int(time.time()*1000)
        log(f"[바이낸스 시간동기] offset={_bn_time_offset_ms}ms (testnet={BINANCE_TESTNET})")
    except Exception as e:
        _bn_time_offset_ms = 0
        log(f"[시간동기 실패] {e}")

def _bn_signed_get(path: str, params: Optional[Dict[str, Any]] = None, retry=_MAX_TS_RETRY):
    if params is None: params = {}
    params["timestamp"] = _bn_now_ms()
    params["recvWindow"] = RECV_WINDOW
    url = f"{BASE_FAPI}{path}?{_bn_sign(params)}"
    r = HTTP.get(url, headers=_bn_headers(), timeout=TIMEOUT)
    if r.status_code == 400 and retry > 0 and _is_ts_error(r.text):
        log("[경고] -1021 감지(GET) → 시간 재동기화 후 재시도")
        sync_binance_time()
        return _bn_signed_get(path, params, retry-1)
    return r

def _bn_signed_post(path: str, params: Optional[Dict[str, Any]] = None, retry=_MAX_TS_RETRY):
    if params is None: params = {}
    params["timestamp"] = _bn_now_ms()
    params["recvWindow"] = RECV_WINDOW
    url = f"{BASE_FAPI}{path}"
    body = _bn_sign(params)
    r = HTTP.post(url, headers=_bn_headers(), data=body, timeout=TIMEOUT)
    if r.status_code == 400 and retry > 0 and _is_ts_error(r.text):
        log("[경고] -1021 감지(POST) → 시간 재동기화 후 재시도")
        sync_binance_time()
        return _bn_signed_post(path, params, retry-1)
    return r


# ===================== Price & FX =====================
def get_upbit_price() -> float:
    url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
    r = HTTP.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()[0]["trade_price"])

def get_binance_price() -> float:
    r = HTTP.get(f"{BASE_FAPI}/fapi/v1/ticker/price", params={"symbol": "BTCUSDT"}, timeout=TIMEOUT)
    r.raise_for_status()
    return float(r.json()["price"])

# ---- Google Finance 환율(USD→KRW) 사용: 30초 캐시 + 폴백 ----
_FX_CACHE_TS: float = 0.0
_FX_CACHE_VAL: float = 1300.0
_FX_TTL_SEC: float = 30.0

def get_usdkrw() -> float:
    """
    1순위: Google Finance USD-KRW (data-last-price 파싱)
    2순위: Upbit KRW-USDT
    3순위: 1300.0 (고정)
    """
    global _FX_CACHE_TS, _FX_CACHE_VAL
    now = time.time()
    if now - _FX_CACHE_TS < _FX_TTL_SEC:
        return _FX_CACHE_VAL

    # Google Finance
    try:
        url = "https://www.google.com/finance/quote/USD-KRW"
        headers = {"User-Agent": "Mozilla/5.0"}
        html = HTTP.get(url, headers=headers, timeout=TIMEOUT).text
        key = 'data-last-price="'
        i = html.find(key)
        if i != -1:
            i += len(key)
            j = html.find('"', i)
            fx = float(html[i:j].replace(",", ""))
            _FX_CACHE_TS, _FX_CACHE_VAL = now, fx
            return fx
        log("[FX 경고] Google Finance 파싱 실패 → KRW-USDT 폴백 시도")
    except Exception as e:
        log(f"[FX 오류] Google Finance 실패: {e} → KRW-USDT 폴백 시도")

    # Upbit KRW-USDT 폴백
    try:
        r = HTTP.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=TIMEOUT)
        r.raise_for_status()
        fx = float(r.json()[0]["trade_price"])
        _FX_CACHE_TS, _FX_CACHE_VAL = now, fx
        return fx
    except Exception as e:
        log(f"[FX 오류] KRW-USDT도 실패: {e} → 1300 사용")

    _FX_CACHE_TS, _FX_CACHE_VAL = now, 1300.0
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
    """USDT-M 선물 지갑 잔고(balance). 에러 로깅을 상세하게 남김."""
    try:
        r = _bn_signed_get("/fapi/v2/balance")
        if r.status_code != 200:
            log(f"[BINANCE 잔고 오류] status={r.status_code}, body={r.text}")
            if '"code":-2015' in r.text:
                global last_error
                last_error = "binance_invalid_key_or_permission (-2015)"
            return 0.0
        for b in r.json():
            if b.get("asset") == "USDT":
                return round(float(b.get("balance", 0.0)), 3)
        return 0.0
    except Exception as e:
        log(f"[BINANCE 잔고 예외] {e}")
        return 0.0


# ===================== Orders & Fills Helpers =====================
def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        r = _bn_signed_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        log(f"[바이낸스 레버리지 설정] {symbol} x{leverage} → status={r.status_code}, body={r.text}")
        if r.status_code != 200 and '"code":-2015' in r.text:
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return r.status_code == 200
    except Exception as e:
        log(f"[레버리지 설정 오류] {e}")
        return False

# --- Upbit 주문: uuid 반환 ---
def upbit_order(side: str, price: float, volume: float) -> Tuple[bool, Optional[str]]:
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
    try:
        data = r.json()
    except Exception:
        data = {"raw": r.text}
    log(f"[업비트 주문] {side.upper()} vol={query.get('volume','')} price={query.get('price','')} → {data}")
    if r.status_code == 201 and isinstance(data, dict) and "uuid" in data:
        return True, data["uuid"]
    return False, None

def upbit_order_detail(uuid_str: str) -> dict:
    keys = load_api_keys()
    access_key = keys.get("upbit_key", "")
    secret_key = keys.get("upbit_secret", "")
    q = {"uuid": uuid_str}

    m = hashlib.sha512(); m.update(urlencode(q).encode())
    payload = {
        "access_key": access_key, "nonce": str(uuid.uuid4()),
        "query_hash": m.hexdigest(), "query_hash_alg": "SHA512",
    }
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    headers = {"Authorization": f"Bearer {jwt_token}"}

    r = HTTP.get("https://api.upbit.com/v1/order", headers=headers, params=q, timeout=TIMEOUT)
    return r.json() if r.status_code == 200 else {"error": r.text}

def summarize_upbit_order(uuid_str: str) -> Tuple[float, float, float]:
    """
    반환: (KRW 총액, BTC 체결수량, KRW 수수료)
    bid: buy KRW(+), ask: sell KRW(+)
    """
    d = upbit_order_detail(uuid_str)
    paid_fee = float(d.get("paid_fee", "0") or 0.0)
    total_funds = 0.0
    total_volume = 0.0
    for t in d.get("trades", []):
        vol = float(t.get("volume", "0") or 0.0)
        funds = float(t.get("funds", "0") or 0.0)
        total_volume += vol
        total_funds += funds
    return total_funds, total_volume, paid_fee

# --- Binance ---
def get_binance_position_qty() -> float:
    r = _bn_signed_get("/fapi/v2/positionRisk")
    if r.status_code == 200:
        for p in r.json():
            if p.get("symbol") == "BTCUSDT":
                try:
                    return float(p.get("positionAmt", 0.0))
                except Exception:
                    return 0.0
    return 0.0

def binance_order(side: str, price: float, volume: float, reduce_only: bool=False) -> Tuple[bool, Optional[int]]:
    try:
        params = {
            "symbol": "BTCUSDT",
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(float(volume), ".3f"),
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        r = _bn_signed_post("/fapi/v1/order", params)
        try:
            data = r.json()
        except Exception:
            data = {"raw": r.text}
        log(f"[바이낸스 주문] {side.upper()} vol={format(float(volume),'.3f')} reduceOnly={reduce_only} @ {price} → status={r.status_code}, body={data}")
        if r.status_code == 200 and isinstance(data, dict) and "orderId" in data:
            return True, int(data["orderId"])
        if r.status_code != 200 and '"code":-2015' in r.text:
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return False, None
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False, None

def binance_user_trades(order_id: int) -> List[dict]:
    params = {"symbol": "BTCUSDT", "orderId": order_id, "timestamp": _bn_now_ms()}
    r = _bn_signed_get("/fapi/v1/userTrades", params)
    return r.json() if r.status_code == 200 else []

def summarize_binance_order(order_id: int) -> Tuple[float, float, float]:
    """
    반환: (총 체결수량 BTC, 체결평균가 USDT, 수수료합 USDT)
    숏 진입(sell): qty>0로 누적
    """
    fills = binance_user_trades(order_id)
    qty = 0.0
    quote = 0.0
    fee = 0.0
    for f in fills:
        q = float(f.get("qty", "0") or 0.0)
        p = float(f.get("price", "0") or 0.0)
        commission = float(f.get("commission", "0") or 0.0)
        qty += q
        quote += q * p
        fee += commission
    avg = (quote / qty) if qty > 0 else 0.0
    return qty, avg, fee


# ===================== Exit helpers (full exit + PnL) =====================
def full_exit_with_retries(max_retries: int = 6, retry_delay: float = 1.0) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    둘 다 0이 될 때까지 반복 청산.
    반환: (성공여부, upbit_sell_uuid, binance_buy_order_id)
    """
    up_uuid = None
    bn_oid = None
    for attempt in range(1, max_retries + 1):
        raw_upbit_bal = get_upbit_balance_real()[1]  # BTC
        upbit_bal = round(raw_upbit_bal, 6)
        pos_amt = get_binance_position_qty()  # 숏<0
        need_up = upbit_bal > 0.00005
        need_bn = pos_amt < -1e-6

        log(f"[청산 체크#{attempt}] upbit={upbit_bal:.6f} BTC, binance_pos={pos_amt:.6f}")

        ok_u = True
        if need_up:
            ok_u, up_uuid = upbit_order("sell", 0.0, upbit_bal)

        ok_b = True
        if need_bn:
            ok_b, bn_oid = binance_order("buy", 0.0, abs(pos_amt), reduce_only=True)

        time.sleep(retry_delay)

        new_up = round(get_upbit_balance_real()[1], 6)
        new_pos = get_binance_position_qty()
        closed = (new_up <= 0.00005) and (-1e-6 <= new_pos <= 1e-6)
        log(f"[청산 재확인#{attempt}] upbit={new_up:.6f}, binance_pos={new_pos:.6f} → {'OK' if closed else 'RETRY'}")

        if ok_u and ok_b and closed:
            return True, up_uuid, bn_oid
    return False, up_uuid, bn_oid

def compute_cycle_pnl_and_log(amount_krw: float, fx_for_exit: float) -> float:
    """
    current_cycle 채워진 값들을 기반으로 실현손익/수수료 누적.
    반환: 이번 사이클 KRW PnL
    """
    global fees_upbit_krw_cum, fees_binance_usdt_cum, fees_binance_krw_cum  # ★ KRW 누적도 갱신

    # Upbit fills
    if current_cycle["upbit_buy_uuid"]:
        krw, qty, fee = summarize_upbit_order(current_cycle["upbit_buy_uuid"])
        current_cycle["upbit_buy_krw"] = krw
        current_cycle["upbit_fee_krw"] += fee
        entry_info["upbit_qty"] = qty

    if current_cycle["upbit_sell_uuid"]:
        krw, qty, fee = summarize_upbit_order(current_cycle["upbit_sell_uuid"])
        current_cycle["upbit_sell_krw"] = krw
        current_cycle["upbit_fee_krw"] += fee

    upbit_pnl = (current_cycle["upbit_sell_krw"] - current_cycle["upbit_buy_krw"] - current_cycle["upbit_fee_krw"])

    # Binance fills
    if current_cycle["binance_sell_id"]:
        qty, avg, fee = summarize_binance_order(current_cycle["binance_sell_id"])
        current_cycle["binance_entry_qty"] = qty
        current_cycle["binance_entry_avg"] = avg
        current_cycle["binance_fee_usdt"] += fee
        entry_info["binance_qty"] = qty

    if current_cycle["binance_buy_id"]:
        qty, avg, fee = summarize_binance_order(current_cycle["binance_buy_id"])
        current_cycle["binance_exit_qty"] = qty
        current_cycle["binance_exit_avg"] = avg
        current_cycle["binance_fee_usdt"] += fee

    qty_close = min(current_cycle["binance_entry_qty"], current_cycle["binance_exit_qty"])
    fut_pnl_usdt = (current_cycle["binance_entry_avg"] - current_cycle["binance_exit_avg"]) * qty_close - current_cycle["binance_fee_usdt"]

    total_krw = upbit_pnl + fut_pnl_usdt * fx_for_exit

    # ★ 누적 수수료 반영 (USDT와 KRW 모두)
    fees_upbit_krw_cum    += current_cycle["upbit_fee_krw"]
    fees_binance_usdt_cum += current_cycle["binance_fee_usdt"]
    fees_binance_krw_cum  += current_cycle["binance_fee_usdt"] * fx_for_exit  # ★ 환율로 KRW 누적

    log(
        f"[실현손익] {total_krw:+,.0f} KRW | Upbit {upbit_pnl:+,.0f} KRW "
        f"+ Binance {fut_pnl_usdt:+.3f} USDT @ FX={fx_for_exit:.2f} | "
        f"수수료 누적 Upbit {fees_upbit_krw_cum:,.0f} KRW / "
        f"Bin {fees_binance_usdt_cum:.3f} USDT ≈ {fees_binance_krw_cum:,.0f} KRW"
    )
    return total_krw

def reset_cycle():
    for k in list(current_cycle.keys()):
        if isinstance(current_cycle[k], (int, float)):
            current_cycle[k] = 0 if isinstance(current_cycle[k], int) else 0.0
        else:
            current_cycle[k] = None


# ===================== Strategy =====================
def _cooldown() -> bool:
    global _last_action_ts
    now = time.time()
    if now - _last_action_ts < COOLDOWN_SEC:
        return True
    _last_action_ts = now
    return False

def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global running, position_state, trade_count, entry_info, last_error, entry_kimp_value, profit_krw

    target_kimp = float(cfg["target_kimp"])
    exit_kimp = float(cfg["exit_kimp"])
    tolerance = float(cfg["tolerance"])
    amount_krw = float(cfg["amount_krw"])
    leverage = int(float(cfg["leverage"]))

    # 바이낸스 준비(시간동기 + 레버리지)
    sync_binance_time()
    set_binance_leverage("BTCUSDT", leverage)

    entry_upbit_qty = 0.0
    entry_binance_qty = 0.0

    while running:
        try:
            if _cooldown():
                time.sleep(0.3)
                continue

            # 잔고 폴링(실패해도 진행)
            _ = get_binance_balance_real()

            kimp, up, bp, fx = calc_kimp()

            # -------- Entry --------
            if position_state == "neutral" and abs(kimp - target_kimp) <= tolerance:
                # 목표 노출: amount_krw와 동일한 명목(USDT)
                target_size_usdt = amount_krw / fx
                need_margin_usdt = target_size_usdt / max(1, leverage)

                bn_usdt = get_binance_balance_real()
                if bn_usdt + 1e-6 < need_margin_usdt:
                    log(f"[진입 중단] 가용 USDT 부족: have≈{bn_usdt:.2f}, need≈{need_margin_usdt:.2f} (lev x{leverage})")
                    continue

                upbit_qty   = round(amount_krw / up, 3)       # 업비트 수량(참고)
                binance_qty = round(target_size_usdt / bp, 3) # 레버리지로 나누지 않음

                reset_cycle()
                ok_upbit, u_uuid = upbit_order("buy", amount_krw, upbit_qty)
                if not ok_upbit or not u_uuid:
                    log("[진입 실패] 업비트 매수 실패")
                    continue
                current_cycle["upbit_buy_uuid"] = u_uuid

                ok_binance, b_oid = binance_order("sell", bp, binance_qty)
                if not ok_binance or not b_oid:
                    log("[진입 실패] 바이낸스 숏 실패 → 업비트 되돌림")
                    upbit_order("sell", 0.0, upbit_qty)
                    reset_cycle()
                    continue
                current_cycle["binance_sell_id"] = b_oid

                entry_upbit_qty = upbit_qty
                entry_binance_qty = binance_qty
                entry_info.update({"upbit_qty": upbit_qty, "binance_qty": binance_qty})
                position_state = "entered"
                entry_kimp_value = float(kimp)
                last_error = None
                log(f"[진입 성공] Upbit {upbit_qty} / Binance {binance_qty} (명목≈₩{amount_krw:,.0f}, lev x{leverage}, 필요증거금≈{need_margin_usdt:.2f} USDT) | 김프 {kimp}%")
                continue

            # -------- Exit --------
            if position_state == "entered":
                crossed = False
                if entry_kimp_value is not None:
                    crossed = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)
                else:
                    crossed = (kimp >= exit_kimp)

                if crossed:
                    log(f"[청산 트리거] entry={entry_kimp_value}, exit={exit_kimp}, kimp_now={kimp}%")
                    # 전량 청산 루틴(양쪽 모두 0될 때까지)
                    done, up_uuid, bn_oid = full_exit_with_retries(max_retries=6, retry_delay=1.0)
                    if done:
                        if up_uuid: current_cycle["upbit_sell_uuid"] = up_uuid
                        if bn_oid:  current_cycle["binance_buy_id"] = bn_oid

                        pnl_krw = compute_cycle_pnl_and_log(amount_krw=amount_krw, fx_for_exit=fx)
                        profit_krw += pnl_krw

                        trade_count += 1
                        position_state = "neutral"
                        entry_info.update({"upbit_qty": 0.0, "binance_qty": 0.0})
                        entry_kimp_value = None
                        log(f"[청산 완료] 누적수익 {profit_krw:,.0f} KRW")
                        reset_cycle()
                    else:
                        log("[청산 미완료] 잔량 존재 → 다음 루프에서 재시도")
                continue

        except Exception as e:
            last_error = str(e)
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
    # ★ 실시간 환율로 본 현재환산 수수료(옵션): 누적 USDT → 현재 KRW
    fx_now = get_usdkrw()
    fees_binance_krw_now = fees_binance_usdt_cum * fx_now

    return jsonify(
        {
            "running": running,
            "position_state": position_state,
            "trade_count": trade_count,
            "logs": logs[-250:],
            "entry_info": entry_info,
            "last_error": last_error,
            "binance_testnet": BINANCE_TESTNET,
            "binance_base": BASE_FAPI,
            "entry_kimp_value": entry_kimp_value,

            # ★ UI에서 바로 쓰세요
            "pnl": {
                "profit_krw_cum": round(profit_krw, 0),
                "fees_upbit_krw_cum": round(fees_upbit_krw_cum, 0),
                "fees_binance_usdt_cum": round(fees_binance_usdt_cum, 3),
                "fees_binance_krw_cum": round(fees_binance_krw_cum, 0),     # ★ 청산시점 환율로 누적
                "fees_binance_krw_now": round(fees_binance_krw_now, 0),     # ★ 현재 환율로 즉시 환산 (참고용)
            },
            # 참고: 마지막 사이클 요약(디버깅용)
            "last_cycle": {
                "upbit_buy_uuid": current_cycle["upbit_buy_uuid"],
                "upbit_sell_uuid": current_cycle["upbit_sell_uuid"],
                "binance_sell_id": current_cycle["binance_sell_id"],
                "binance_buy_id": current_cycle["binance_buy_id"],
            }
        }
    )

@app.route("/start", methods=["POST"])
def start():
    global running, last_error
    if not running:
        # 키 존재 선검사
        keys = load_api_keys()
        if not keys.get("binance_key") or not keys.get("binance_secret"):
            last_error = "binance_api_key_missing"
            log("[전략 시작 실패] BINANCE_API_KEY/SECRET 누락")
            return jsonify({"status": "error", "error": last_error}), 400

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
        log(f"[전략 시작] cfg={cfg} testnet={BINANCE_TESTNET} base={BASE_FAPI}")
    return jsonify({"status": "started"})

@app.route("/stop", methods=["POST"])
def stop():
    global running
    running = False
    log("[전략 중지]")
    return jsonify({"status": "stopped"})


if __name__ == "__main__":
    # 부팅 시 1회 시간동기(선제)
    sync_binance_time()
    app.run(host="0.0.0.0", port=5000, debug=False)
