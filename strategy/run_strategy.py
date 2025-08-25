# run_strategy.py
from __future__ import annotations
import os, time, threading, requests, hmac, hashlib, uuid, jwt
from urllib.parse import urlencode
from typing import Any, Dict, Tuple, Optional

# =============== 전역 상태 ===============
running: bool = False
position_state: str = "neutral"      # neutral / entered
trade_count: int = 0
profit_krw: float = 0.0              # 누적 실현손익(KRW)
total_pnl: float = 0.0               # (확장용, 현재는 profit_krw와 동일하게만 사용 가능)
logs: list[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}
entry_kimp_value: float | None = None
last_error: str | None = None

# UI용 포지션 스냅샷
current_position: Optional[Dict[str, Any]] = None  # live=True/False 포함
last_position: Optional[Dict[str, Any]] = None     # 가장 최근 종료 포지션 스냅샷

# 거래소 스텝/상수
UPBIT_QTY_STEP = 0.0001
BINANCE_QTY_STEP = 0.001
BINANCE_SYMBOL = "BTCUSDT"
RECV_WINDOW = 5000  # ms
MAX_RETRY_ON_1021 = 1

# 테스트넷/메인넷 자동 분기
BINANCE_TESTNET = str(os.getenv("BINANCE_TESTNET", "false")).lower() in ("1", "true", "yes")
BASE_FAPI = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"

# 바이낸스 시간 오프셋(ms)
_binance_time_offset_ms: int = 0

# PnL/원자성 추적
state_lock = threading.RLock()
entry: Dict[str, Any] = {
    "ts": None, "qty": 0.0, "ub_avg": 0.0, "ub_fee_krw": 0.0,
    "bn_avg": 0.0, "bn_fee_usdt": 0.0, "usdkrw_entry": 0.0,
    "kimp_entry": None, "krw_spent": 0.0, "bn_notional_usdt": 0.0, "lev": 3
}
last_realized_krw: float = 0.0
last_roi_pct: float = 0.0

DUST_BTC = 1e-6  # 미세 잔량 경계

# =============== 공통 유틸 ===============
def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 400:
        del logs[:200]

def floor_step(x: float, step: float) -> float:
    return int(float(x) / step) * step

def get_upbit_price() -> float:
    r = requests.get("https://api.upbit.com/v1/ticker?markets=KRW-BTC", timeout=10)
    r.raise_for_status()
    return float(r.json()[0]['trade_price'])

def get_binance_price() -> float:
    r = requests.get(f"{BASE_FAPI}/fapi/v1/ticker/price", params={"symbol": BINANCE_SYMBOL}, timeout=10)
    r.raise_for_status()
    return float(r.json()['price'])

def get_usdkrw() -> float:
    try:
        r = requests.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=5)
        return float(r.json()[0]["trade_price"])
    except Exception:
        return 1300.0

# =============== Upbit ===============
def _upbit_headers(query: dict | None = None) -> dict:
    from api.api_key import load_api_keys
    keys = load_api_keys()
    access_key, secret_key = keys.get('upbit_key', ""), keys.get('upbit_secret', "")
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    if query is not None:
        q = urlencode(query).encode()
        m = hashlib.sha512(); m.update(q)
        payload["query_hash"] = m.hexdigest()
        payload["query_hash_alg"] = "SHA512"
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    return {"Authorization": f"Bearer {jwt_token}"}

def get_upbit_btc_balance() -> float:
    r = requests.get("https://api.upbit.com/v1/accounts", headers=_upbit_headers(), timeout=10)
    r.raise_for_status()
    for a in r.json():
        if a["currency"] == "BTC":
            return float(a["balance"])  # locked 제외
    return 0.0

def upbit_get_order(uuid_: str) -> dict:
    q = {"uuid": uuid_}
    r = requests.get("https://api.upbit.com/v1/order", params=q, headers=_upbit_headers(q), timeout=10)
    r.raise_for_status()
    return r.json()

def upbit_get_fill(uuid_: str) -> dict:
    """
    반환: {"filled_qty": float, "avg_price": float, "fee_krw": float}
    (시장가 주문 기준 / paid_fee는 매수: KRW, 매도: BTC일 수 있음)
    """
    od = upbit_get_order(uuid_)
    filled = float(od.get("executed_volume", 0.0) or 0.0)
    fee = float(od.get("paid_fee", 0.0) or 0.0)
    trades = od.get("trades", []) or []
    notional = 0.0
    for t in trades:
        px = float(t.get("price", 0.0)); vol = float(t.get("volume", 0.0))
        notional += px * vol
    avg = (notional / filled) if filled > 0 else 0.0
    if od.get("side") == "bid":  # 매수면 KRW 수수료
        fee_krw = fee
    else:                       # 매도면 BTC 수수료 → KRW 환산
        fee_krw = fee * get_upbit_price()
    return {"filled_qty": filled, "avg_price": avg, "fee_krw": fee_krw}

def upbit_market_buy_by_krw(amount_krw: float) -> tuple[bool, float, str, str]:
    before = get_upbit_btc_balance()
    q = {"market":"KRW-BTC","side":"bid","ord_type":"price","price":str(int(amount_krw))}
    r = requests.post("https://api.upbit.com/v1/orders", params=q, headers=_upbit_headers(q), timeout=10)
    text = r.text
    log(f"[업비트 매수] KRW={int(amount_krw)} → {text[:200]}")
    if r.status_code != 201:
        return False, 0.0, text, ""
    uuid_ = ""
    try:
        uuid_ = r.json().get("uuid", "")
    except Exception:
        pass
    # 잔고차로 체결 수량 1차 추정
    got = 0.0
    for _ in range(8):
        time.sleep(0.5)
        after = get_upbit_btc_balance()
        got = max(0.0, round(after - before, 8))
        if got > 0:
            break
    return (got > 0), got, text, uuid_

def upbit_market_sell_btc(volume_btc: float) -> tuple[bool, str, str]:
    vol = format(floor_step(volume_btc, UPBIT_QTY_STEP), ".8f")
    q = {"market":"KRW-BTC","side":"ask","ord_type":"market","volume":vol}
    r = requests.post("https://api.upbit.com/v1/orders", params=q, headers=_upbit_headers(q), timeout=10)
    text = r.text
    log(f"[업비트 매도] vol={vol} → {text[:200]}")
    uuid_ = ""
    try:
        if r.status_code == 201:
            uuid_ = r.json().get("uuid", "")
    except Exception:
        pass
    return (r.status_code == 201), text, uuid_

# =============== Binance Futures 공통 ===============
def _binance_keys() -> Tuple[str, str]:
    from api.api_key import load_api_keys
    k = load_api_keys()
    return k.get('binance_key', ""), k.get('binance_secret', "")

def _bn_now_ms() -> int:
    return int(time.time()*1000) + _binance_time_offset_ms

def _bn_headers() -> dict:
    api_key, _ = _binance_keys()
    return {"X-MBX-APIKEY": api_key, "Content-Type": "application/x-www-form-urlencoded"}

def _bn_sign(params: Dict[str, Any]) -> str:
    _, sec = _binance_keys()
    qs = urlencode(params, doseq=True)
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def _is_ts_error(resp_text: str) -> bool:
    t = (resp_text or "").lower()
    return "-1021" in t or ("timestamp" in t and ("ahead" in t or "recvwindow" in t or "server time" in t))

def sync_binance_server_time() -> None:
    global _binance_time_offset_ms
    try:
        r = requests.get(f"{BASE_FAPI}/fapi/v1/time", timeout=5)
        r.raise_for_status()
        st = int(r.json()["serverTime"])
        _binance_time_offset_ms = st - int(time.time()*1000)
        log(f"[바이낸스 시간동기] offset={_binance_time_offset_ms}ms (testnet={BINANCE_TESTNET})")
    except Exception as e:
        log(f"[시간동기 실패] {e}")
        _binance_time_offset_ms = 0

def _bn_signed_get(path: str, params: Optional[Dict[str, Any]]=None, retry:int=MAX_RETRY_ON_1021):
    if params is None: params = {}
    params["timestamp"] = _bn_now_ms()
    params["recvWindow"] = RECV_WINDOW
    url = f"{BASE_FAPI}{path}?{_bn_sign(params)}"
    r = requests.get(url, headers=_bn_headers(), timeout=10)
    if r.status_code == 400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021 감지(GET) → 시간 재동기화 후 재시도")
        sync_binance_server_time()
        return _bn_signed_get(path, params, retry-1)
    return r

def _bn_signed_post(path: str, params: Optional[Dict[str, Any]]=None, retry:int=MAX_RETRY_ON_1021):
    if params is None: params = {}
    params["timestamp"] = _bn_now_ms()
    params["recvWindow"] = RECV_WINDOW
    url = f"{BASE_FAPI}{path}"
    body = _bn_sign(params)
    r = requests.post(url, headers=_bn_headers(), data=body, timeout=10)
    if r.status_code == 400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021 감지(POST) → 시간 재동기화 후 재시도")
        sync_binance_server_time()
        return _bn_signed_post(path, params, retry-1)
    return r

# =============== Binance 기능 ===============
def ensure_one_way_mode() -> bool:
    try:
        r = _bn_signed_post("/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        log(f"[바이낸스 모드] 원웨이 설정 → {r.status_code}, {r.text[:160]}")
        if r.status_code == 200:
            return True
        if r.status_code in (401, 403) or '"code":-2015' in r.text:
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return False
    except Exception as e:
        log(f"[모드설정 오류] {e}")
        return False

def set_margin_mode(symbol: str, isolated: bool=True) -> bool:
    try:
        r = _bn_signed_post("/fapi/v1/marginType", {"symbol": symbol, "marginType": ("ISOLATED" if isolated else "CROSSED")})
        ok_text = r.text.lower()
        ok = (r.status_code == 200) or ("no need to change" in ok_text) or ("margin type same" in ok_text)
        log(f"[마진모드] ISOLATED={isolated} → {r.status_code}, {r.text[:160]}")
        if not ok and ('"code":-2015' in r.text):
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return ok
    except Exception as e:
        log(f"[마진모드 오류] {e}")
        return False

def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        r = _bn_signed_post("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        log(f"[레버리지] {symbol} x{leverage} → {r.status_code}, {r.text[:160]}")
        if r.status_code != 200 and ('"code":-2015' in r.text):
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return r.status_code == 200
    except Exception as e:
        log(f"[레버리지 오류] {e}")
        return False

def get_binance_position_qty() -> float:
    try:
        r = _bn_signed_get("/fapi/v2/positionRisk")
        if r.status_code != 200:
            log(f"[포지션 조회 실패] {r.status_code}, {r.text[:200]}")
            return 0.0
        for p in r.json():
            if p.get("symbol") == BINANCE_SYMBOL:
                return float(p.get("positionAmt", 0.0))  # 숏<0, 롱>0
    except Exception as e:
        log(f"[포지션 조회 예외] {e}")
    return 0.0

def get_binance_futures_usdt_balance() -> Dict[str, float] | Dict[str, str]:
    try:
        r = _bn_signed_get("/fapi/v2/balance")
        if r.status_code != 200:
            log(f"[잔고 조회 실패] {r.status_code}, {r.text[:200]}")
            if '"code":-2015' in r.text:
                global last_error
                last_error = "binance_invalid_key_or_permission (-2015)"
            return {"error": r.text}
        balances = r.json()
        usdt = next((x for x in balances if x.get("asset") == "USDT"), None)
        if not usdt:
            return {"error": "USDT balance not found"}
        return {"total": float(usdt.get("balance", 0.0)), "available": float(usdt.get("availableBalance", 0.0))}
    except Exception as e:
        log(f"[잔고 조회 예외] {e}")
        return {"error": str(e)}

def binance_market_order_id(side: str, qty: float, reduce_only: bool=False) -> tuple[bool, Optional[int], str]:
    try:
        q = floor_step(qty, BINANCE_QTY_STEP)
        if q <= 0:
            return True, None, "qty<=0"
        params = {
            "symbol": BINANCE_SYMBOL,
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(q, ".3f"),
            "newOrderRespType": "RESULT",
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        r = _bn_signed_post("/fapi/v1/order", params)
        log(f"[바이낸스 주문] {side.upper()} {params['quantity']} reduceOnly={reduce_only} → {r.status_code} {r.text[:200]}")
        if r.status_code != 200 and ('"code":-2015' in r.text):
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        oid = None
        try:
            if r.status_code == 200:
                oid = int(r.json().get("orderId"))
        except Exception:
            pass
        return r.status_code == 200, oid, r.text
    except Exception as e:
        log(f"[바이낸스 주문 예외] {e}")
        return False, None, str(e)

def binance_futures_get_fill(order_id: int) -> dict:
    """해당 주문의 체결 리스트 합산 → avg/qty/fee(USDT)"""
    try:
        r = _bn_signed_get("/fapi/v1/userTrades", {"symbol": BINANCE_SYMBOL, "orderId": order_id})
        if r.status_code != 200:
            log(f"[fills 실패] {r.status_code} {r.text[:200]}")
            return {"filled_qty": 0.0, "avg_price": 0.0, "fee_usdt": 0.0}
        trades = r.json()
        qty = 0.0; notional = 0.0; fee = 0.0
        for t in trades:
            q = float(t.get("qty", 0.0)); p = float(t.get("price", 0.0)); c = float(t.get("commission", 0.0))
            qty += q; notional += q * p; fee += c
        avg = (notional/qty) if qty>0 else 0.0
        return {"filled_qty": qty, "avg_price": avg, "fee_usdt": fee}
    except Exception as e:
        log(f"[fills 예외] {e}")
        return {"filled_qty": 0.0, "avg_price": 0.0, "fee_usdt": 0.0}

# =============== 시장 스냅샷/김프 ===============
def get_market_snapshot() -> Dict[str, Any]:
    try:
        up = get_upbit_price(); bi = get_binance_price(); fx = get_usdkrw()
        kimp = (up - bi * fx) / (bi * fx) * 100.0
        return {"upbit_price": up, "binance_price": bi, "usdkrw": fx, "kimp": kimp}
    except Exception as e:
        return {"error": str(e)}

# =============== 손익 계산 ===============
def compute_realized_pnl_krw(
    qty: float,
    ub_buy_avg: float, ub_sell_avg: float,
    ub_fee_in_krw: float, ub_fee_out_krw: float,
    bn_sell_avg: float, bn_buy_avg: float,
    bn_fee_in_usdt: float, bn_fee_out_usdt: float,
    usdkrw_exit: float, funding_usdt: float = 0.0
) -> float:
    # 업비트 현물
    krw_in  = ub_buy_avg  * qty + ub_fee_in_krw
    krw_out = ub_sell_avg * qty - ub_fee_out_krw
    upbit_realized = krw_out - krw_in
    # 바이낸스 선물(숏: 진입가-청산가)
    usdt_pnl = (bn_sell_avg - bn_buy_avg) * qty
    usdt_realized = usdt_pnl - (bn_fee_in_usdt + bn_fee_out_usdt + funding_usdt)
    return upbit_realized + usdt_realized * usdkrw_exit

def compute_roi_pct_from_entry(entry_: dict, realized_krw: float) -> float:
    if not entry_ or not entry_.get("krw_spent") or not entry_.get("usdkrw_entry"):
        return 0.0
    capital = entry_["krw_spent"] + (entry_["bn_notional_usdt"]/ max(1, entry_["lev"])) * entry_["usdkrw_entry"]
    return (realized_krw / capital) * 100.0 if capital > 0 else 0.0

# =============== 잔량 스윕/진입/청산 ===============
def _sweep_all_residuals():
    ub_btc = get_upbit_btc_balance()
    if ub_btc > DUST_BTC:
        upbit_market_sell_btc(ub_btc)
    pos = get_binance_position_qty()
    if pos > DUST_BTC:
        binance_market_order_id("sell", pos, reduce_only=True)
    elif pos < -DUST_BTC:
        binance_market_order_id("buy", -pos, reduce_only=True)

def must_enter(amount_krw: float, poll_sec: float=0.5, leverage:int=3, tol_usdt:float=5.0) -> Tuple[float, float]:
    snap = get_market_snapshot()
    if "error" in snap:
        log(f"[진입 중단] 스냅샷 오류: {snap['error']}")
        return 0.0, 0.0
    up = snap["upbit_price"]; bi = snap["binance_price"]; fx = snap["usdkrw"]
    kimp_now = (up - bi * fx) / (bi * fx) * 100.0

    target_size_usdt = amount_krw / fx
    need_margin_usdt = target_size_usdt / max(1, leverage)

    bal = get_binance_futures_usdt_balance()
    if isinstance(bal, dict) and "available" in bal:
        if float(bal["available"]) + 1e-6 < need_margin_usdt:
            log(f"[진입 중단] 가용USDT 부족")
            return 0.0, 0.0

    # 업비트 매수 (uuid 확보)
    ok_u, got, _, ub_uuid = upbit_market_buy_by_krw(amount_krw)
    if not ok_u or got <= 0:
        log("[진입 실패] 업비트 체결 실패")
        return 0.0, 0.0
    ub_fill = upbit_get_fill(ub_uuid)
    upbit_qty = floor_step(ub_fill["filled_qty"], UPBIT_QTY_STEP)
    if upbit_qty <= 0:
        log("[진입 실패] 업비트 수량 0")
        return 0.0, 0.0

    # 바이낸스 숏
    target_qty = floor_step(target_size_usdt / bi, BINANCE_QTY_STEP)
    ok_b, oid, _ = binance_market_order_id("sell", target_qty, reduce_only=False)
    if not ok_b or not oid:
        log("[진입 실패] 바이낸스 주문 실패 → 업비트 보상매도")
        upbit_market_sell_btc(upbit_qty)
        return 0.0, 0.0
    bn_fill = binance_futures_get_fill(oid)

    # 명목 USDT 보정 루프
    for _ in range(12):
        pos = abs(get_binance_position_qty()); cur_usdt = pos * bi
        diff = target_size_usdt - cur_usdt
        if abs(diff) <= tol_usdt:
            break
        adj_qty = floor_step(abs(diff)/bi, BINANCE_QTY_STEP)
        if adj_qty <= 0:
            break
        if diff > 0:
            ok_b2, oid2, _ = binance_market_order_id("sell", adj_qty, reduce_only=False)
        else:
            ok_b2, oid2, _ = binance_market_order_id("buy", adj_qty, reduce_only=True)
        if ok_b2 and oid2:
            f2 = binance_futures_get_fill(oid2)
            tot_q = bn_fill["filled_qty"] + f2["filled_qty"]
            if tot_q > 0:
                bn_fill["avg_price"] = (bn_fill["avg_price"]*bn_fill["filled_qty"] + f2["avg_price"]*f2["filled_qty"]) / tot_q
                bn_fill["filled_qty"] = tot_q
                bn_fill["fee_usdt"] += f2["fee_usdt"]
        time.sleep(poll_sec)

    q = min(upbit_qty, floor_step(abs(get_binance_position_qty()), BINANCE_QTY_STEP))
    if q <= 0:
        log("[진입 실패] 양다리 수량 불일치 → 보상거래")
        upbit_market_sell_btc(upbit_qty)
        return 0.0, 0.0

    # 엔트리 스냅샷
    with state_lock:
        entry.update({
            "ts": time.time(),
            "qty": q,
            "ub_avg": ub_fill["avg_price"],
            "ub_fee_krw": ub_fill["fee_krw"],
            "bn_avg": bn_fill["avg_price"],
            "bn_fee_usdt": bn_fill["fee_usdt"],
            "usdkrw_entry": fx,
            "kimp_entry": kimp_now,
            "krw_spent": ub_fill["avg_price"]*q + ub_fill["fee_krw"],
            "bn_notional_usdt": bn_fill["avg_price"]*q,
            "lev": leverage
        })
    return upbit_qty, floor_step(abs(get_binance_position_qty()), BINANCE_QTY_STEP)

def must_exit(poll_sec: float=0.5):
    global profit_krw, last_realized_krw, last_roi_pct

    q = float(entry.get("qty", 0.0) or 0.0)
    if q <= 0:
        _sweep_all_residuals()
        return

    # 바이낸스 숏 청산(BUY reduceOnly)
    ok_b, oid_b, _ = binance_market_order_id("buy", q, reduce_only=True)
    time.sleep(poll_sec)
    bn_out = binance_futures_get_fill(oid_b) if (ok_b and oid_b) else {"avg_price": 0.0, "fee_usdt": 0.0}

    # 업비트 전량 매도
    ok_u, _, ub_uuid = upbit_market_sell_btc(q)
    time.sleep(poll_sec)
    ub_out = upbit_get_fill(ub_uuid) if (ok_u and ub_uuid) else {"avg_price": 0.0, "fee_krw": 0.0}

    # 잔량 스윕
    for _ in range(20):
        _sweep_all_residuals()
        time.sleep(poll_sec/2)
        if get_upbit_btc_balance() < 1e-6 and abs(get_binance_position_qty()) < 1e-6:
            break

    # 실현손익 계산
    usdkrw_exit = get_usdkrw()
    realized = compute_realized_pnl_krw(
        qty=q,
        ub_buy_avg=entry["ub_avg"], ub_sell_avg=ub_out["avg_price"],
        ub_fee_in_krw=entry["ub_fee_krw"], ub_fee_out_krw=ub_out["fee_krw"],
        bn_sell_avg=entry["bn_avg"], bn_buy_avg=bn_out["avg_price"],
        bn_fee_in_usdt=entry["bn_fee_usdt"], bn_fee_out_usdt=bn_out["fee_usdt"],
        usdkrw_exit=usdkrw_exit, funding_usdt=0.0
    )
    with state_lock:
        profit_krw += realized
        last_realized_krw = realized
        last_roi_pct = compute_roi_pct_from_entry(entry, realized)
        entry.clear()

# =============== 전략 루프 ===============
def run_strategy_thread(config: Dict[str, Any]) -> None:
    global running, position_state, trade_count, profit_krw, total_pnl
    global entry_info, entry_kimp_value, last_error
    global current_position, last_position

    target_kimp = float(config['target_kimp'])
    exit_kimp   = float(config['exit_kimp'])
    tolerance   = float(config['tolerance'])
    amount_krw  = float(config['amount_krw'])

    exit_on_sign_change: bool = bool(config.get('exit_on_sign_change', True))
    exit_on_move_bp: float = float(config.get('exit_on_move_bp', 0.0))  # 엔트리 대비 n bp(%) 이상
    sticky_display: bool = bool(config.get('sticky_display', True))

    # Binance 준비
    sync_binance_server_time()
    ensure_one_way_mode()
    set_margin_mode(BINANCE_SYMBOL, isolated=True)
    set_binance_leverage(BINANCE_SYMBOL, 3)  # 3배(수량엔 영향 없음)

    while running:
        try:
            _ = get_binance_futures_usdt_balance()  # 표시용 갱신

            snap = get_market_snapshot()
            if "error" in snap:
                last_error = snap["error"]
                time.sleep(1); continue

            up = snap["upbit_price"]; bi = snap["binance_price"]; fx = snap["usdkrw"]
            kimp = (up - bi * fx) / (bi * fx) * 100.0
            kimp_view = round(kimp, 2)
            last_error = None

            if position_state == "neutral":
                dir_ok  = (kimp <= target_kimp) if target_kimp < 0 else (kimp >= target_kimp)
                near_ok = abs(kimp - target_kimp) <= tolerance
                if dir_ok and near_ok:
                    uq, bq = must_enter(amount_krw, poll_sec=0.5, leverage=3, tol_usdt=5.0)
                    if uq > 0 and bq > 0:
                        entry_info["upbit_qty"] = uq
                        entry_info["binance_qty"] = bq
                        position_state = "entered"
                        entry_kimp_value = round(kimp, 4)
                        current_position = {
                            "live": True,
                            "side": "LS",  # Upbit LONG + Binance SHORT
                            "upbit_qty": uq,
                            "binance_qty": bq,
                            "entry_kimp": entry_kimp_value,
                            "entered_at": time.time(),
                        }
                        log(f"[진입 완료] 업비트 +{uq}BTC / 바이낸스 -{bq}BTC @ {kimp_view}% (명목≈{amount_krw:.0f}KRW)")
                    else:
                        last_error = last_error or "binance_enter_failed"

            elif position_state == "entered":
                if entry_kimp_value is not None:
                    crossed_exit = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)
                else:
                    crossed_exit = False

                sign_change = False
                if exit_on_sign_change and entry_kimp_value is not None:
                    sign_change = (entry_kimp_value >= 0 and kimp < 0) or (entry_kimp_value < 0 and kimp >= 0)

                move_trigger = False
                if exit_on_move_bp > 0 and entry_kimp_value is not None:
                    move_trigger = abs(kimp - entry_kimp_value) >= exit_on_move_bp

                if crossed_exit or sign_change or move_trigger:
                    why = "exit_kimp" if crossed_exit else ("sign_change" if sign_change else "move_trigger")
                    log(f"[청산 트리거:{why}] entry={entry_kimp_value}, exit={exit_kimp}, 현재={kimp_view}%")

                    must_exit()
                    trade_count += 1

                    if sticky_display and current_position:
                        current_position.update({
                            "live": False,
                            "closed_at": time.time(),
                            "close_kimp": round(kimp, 4),
                            "realized_krw": round(last_realized_krw),
                            "roi_last_pct": round(last_roi_pct, 3)
                        })
                        last_position = current_position.copy()
                    else:
                        last_position = None

                    position_state = "neutral"
                    entry_info.update({"upbit_qty":0.0,"binance_qty":0.0})
                    entry_kimp_value = None
                    current_position = None
                    log(f"[청산 완료] 실현손익={round(last_realized_krw)} KRW, ROI={round(last_roi_pct,3)}%")

        except Exception as e:
            last_error = str(e)
            log(f"[에러] {e}")

        time.sleep(0.6)

# =============== 외부 인터페이스 ===============
def start_strategy(config: Dict[str, Any]) -> None:
    global running, last_error
    if running:
        return
    # 키 검증
    bk, bs = _binance_keys()
    if not bk or not bs:
        last_error = "binance_api_key_missing"
        log("[시작실패] BINANCE_API_KEY/SECRET 비어있음 (.env 또는 api_key 로드 확인)")
        return
    running = True
    threading.Thread(target=run_strategy_thread, args=(config,), daemon=True).start()
    log(f"[전략 시작] testnet={BINANCE_TESTNET}, base={BASE_FAPI}")

def stop_strategy() -> None:
    global running, entry_kimp_value
    running = False
    entry_kimp_value = None
    log("[전략 중지]")

def force_exit_now() -> None:
    try:
        must_exit()
        log("[수동 청산] 완료")
    except Exception as e:
        log(f"[수동 청산 오류] {e}")

def get_strategy_status() -> Dict[str, Any]:
    snap = get_market_snapshot()
    binance_bal = get_binance_futures_usdt_balance()
    display_position = current_position if current_position is not None else last_position
    display_is_live = bool(display_position and display_position.get("live", False))

    # total_pnl은 확장 전까지 profit_krw로 동일 노출 가능
    return {
        "running": running,
        "position_state": position_state,
        "profit_krw": round(profit_krw),
        "total_pnl": round(profit_krw),
        "trade_count": trade_count,
        "entry_kimp_value": entry_kimp_value,
        "entry_info": entry_info,
        "last_error": last_error,
        "snapshot": snap,
        "balances": {"binance_futures_usdt": binance_bal},
        "current_position": current_position,
        "last_position": last_position,
        "display_position": display_position,
        "display_is_live": display_is_live,
        "last_realized_krw": round(last_realized_krw),
        "roi_last_pct": round(last_roi_pct, 3),
        "logs": logs[-250:],
    }

__all__ = [
    "start_strategy", "stop_strategy", "get_strategy_status",
    "get_market_snapshot", "force_exit_now"
]
