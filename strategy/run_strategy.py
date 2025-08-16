from __future__ import annotations
import time, threading, requests, hmac, hashlib, uuid, jwt, json
from urllib.parse import urlencode
from typing import Any, Dict, Tuple, Optional, List
from api.api_key import load_api_keys

# ===== 전역 상태 =====
running: bool = False
position_state: str = "neutral"      # neutral / entered
exiting: bool = False                # 중복 청산 가드
trade_count: int = 0
profit_krw: float = 0.0              # 누적 실현손익(KRW)
total_pnl: float = 0.0               # (유지: 필요 시 확장)
logs: List[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}
entry_kimp_value: float | None = None

# 청산 직후 재진입 쿨다운
last_exit_ts: float = 0.0
EXIT_COOLDOWN_SEC: float = 3.0

# 사이클 체결/비용 기록
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
    "binance_fee_usdt": 0.0
}

# --------------------- utils ---------------------
def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 400:
        logs.pop(0)

def get_upbit_price() -> float:
    url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
    return float(requests.get(url, timeout=10).json()[0]['trade_price'])

def get_binance_price() -> float:
    url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"
    return float(requests.get(url, timeout=10).json()['price'])

def get_usdkrw() -> float:
    try:
        url = "https://www.google.com/finance/quote/USD-KRW"
        headers = {"User-Agent": "Mozilla/5.0"}
        text = requests.get(url, headers=headers, timeout=10).text
        s = text.find('data-last-price="')
        if s == -1:
            return 1300.0
        s += len('data-last-price="')
        e = text.find('"', s)
        v = float(text[s:e].replace(',', ''))
        return v if 900.0 <= v <= 2000.0 else 1300.0
    except Exception:
        return 1300.0

def calc_kimp(up_krw: float, bi_usdt: float, usdkrw: float) -> float:
    # 김치프리미엄(%) = 업비트KRW / (바이낸스USDT*환율) - 1
    return (up_krw / (bi_usdt * usdkrw) - 1.0) * 100.0

def floor_step(x: float, step: float) -> float:
    return (int(float(x) / step)) * step

# ========== 초고속 마켓 캐시 ==========
class MarketCache:
    def __init__(self):
        self.lock = threading.Lock()
        self.upbit: float = 0.0
        self.binance: float = 0.0
        self.usdkrw: float = 1300.0
        self.ts_upbit: float = 0.0
        self.ts_binance: float = 0.0
        self.ts_fx: float = 0.0

MARKET = MarketCache()
PRICE_STALE_SEC = 2.0     # 시세는 2초 이상이면 스테일
FX_REFRESH_SEC   = 20.0   # 환율은 20초마다 갱신
_market_thread_started = False

def _update_market_loop():
    """업비트/바이낸스는 250ms 주기, 환율은 20s 주기로 갱신"""
    upbit_url = "https://api.upbit.com/v1/ticker?markets=KRW-BTC"
    binance_url = "https://fapi.binance.com/fapi/v1/ticker/price?symbol=BTCUSDT"
    headers_fx = {"User-Agent": "Mozilla/5.0"}

    while True:
        now = time.time()
        try:
            r1 = requests.get(upbit_url, timeout=(1, 2))
            r1.raise_for_status()
            up = float(r1.json()[0]["trade_price"])
            with MARKET.lock:
                MARKET.upbit = up
                MARKET.ts_upbit = now
        except Exception:
            pass

        try:
            r2 = requests.get(binance_url, timeout=(1, 2))
            r2.raise_for_status()
            bi = float(r2.json()["price"])
            with MARKET.lock:
                MARKET.binance = bi
                MARKET.ts_binance = now
        except Exception:
            pass

        try:
            if now - MARKET.ts_fx > FX_REFRESH_SEC:
                html = requests.get("https://www.google.com/finance/quote/USD-KRW",
                                    headers=headers_fx, timeout=(2, 4)).text
                s = html.find('data-last-price="')
                if s != -1:
                    s += len('data-last-price="')
                    e = html.find('"', s)
                    v = float(html[s:e].replace(",", ""))
                    if 900.0 <= v <= 2000.0:
                        with MARKET.lock:
                            MARKET.usdkrw = v
                            MARKET.ts_fx = now
        except Exception:
            pass

        time.sleep(0.25)  # 250ms

def ensure_market_updater():
    global _market_thread_started
    if not _market_thread_started:
        t = threading.Thread(target=_update_market_loop, daemon=True)
        t.start()
        _market_thread_started = True
        log("[마켓캐시] 초고속 업데이트 쓰레드 시작 (0.25s / FX 20s)")

def get_market_snapshot() -> Tuple[float, float, float, float]:
    """
    캐시를 우선 사용해 (kimp, upbit, binance, usdkrw)를 반환.
    캐시가 오래됐으면 1회 폴백 호출로 보정 후 반환.
    """
    now = time.time()
    with MARKET.lock:
        up = MARKET.upbit
        bi = MARKET.binance
        fx = MARKET.usdkrw
        tsu, tsb, tsf = MARKET.ts_upbit, MARKET.ts_binance, MARKET.ts_fx

    if (now - tsu > PRICE_STALE_SEC) or (now - tsb > PRICE_STALE_SEC) or (now - tsf > 120.0):
        # 폴백: 직접 호출(상대적으로 느리지만 1회 보정)
        up = get_upbit_price()
        bi = get_binance_price()
        fx = get_usdkrw()
        with MARKET.lock:
            MARKET.upbit, MARKET.ts_upbit = up, now
            MARKET.binance, MARKET.ts_binance = bi, now
            MARKET.usdkrw, MARKET.ts_fx = fx, now

    k = calc_kimp(up, bi, fx)
    return round(k, 2), up, bi, fx

# ----------------- Upbit -----------------
def upbit_auth_headers(with_query: bool, query: dict | None = None) -> dict:
    keys = load_api_keys()
    access_key, secret_key = keys.get('upbit_key', ""), keys.get('upbit_secret', "")
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    if with_query and query is not None:
        q = urlencode(query).encode()
        m = hashlib.sha512(); m.update(q)
        payload["query_hash"] = m.hexdigest()
        payload["query_hash_alg"] = "SHA512"
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    return {"Authorization": f"Bearer {jwt_token}"}

def get_upbit_btc_balance() -> float:
    headers = upbit_auth_headers(False)
    res = requests.get("https://api.upbit.com/v1/accounts", headers=headers, timeout=10).json()
    for a in res:
        if a["currency"] == "BTC":
            return float(a["balance"])
    return 0.0

def upbit_order(side: str, price_krw: float, volume_btc: float) -> Tuple[bool, Optional[str]]:
    """
    buy: KRW 금액 시장가 / sell: BTC 수량 시장가
    반환: (성공여부, uuid)
    """
    if side == 'buy':
        query = {"market": "KRW-BTC", "side": "bid", "ord_type": "price", "price": str(int(price_krw))}
    else:
        query = {"market": "KRW-BTC", "side": "ask", "ord_type": "market", "volume": format(float(volume_btc), ".4f")}
    headers = upbit_auth_headers(True, query)
    res = requests.post("https://api.upbit.com/v1/orders", params=query, headers=headers, timeout=10)
    try:
        data = res.json()
    except Exception:
        data = {"raw": res.text}
    log(f"[업비트 주문] {side.upper()} vol={query.get('volume','')} price={query.get('price','')} → {json.dumps(data, ensure_ascii=False)}")
    if res.status_code == 201 and isinstance(data, dict) and "uuid" in data:
        return True, data["uuid"]
    return False, None

def upbit_order_detail(uuid_str: str) -> dict:
    q = {"uuid": uuid_str}
    headers = upbit_auth_headers(True, q)
    url = "https://api.upbit.com/v1/order"
    res = requests.get(url, headers=headers, params=q, timeout=10)
    return res.json()

def summarize_upbit_order(uuid_str: str) -> Tuple[float, float, float]:
    """
    uuid 기준으로 체결 합산
    반환: (총KRW금액, 총BTC수량, 수수료KRW)
    """
    d = upbit_order_detail(uuid_str)
    side = d.get("side")
    paid_fee = float(d.get("paid_fee", "0") or 0.0)
    total_funds = 0.0
    total_volume = 0.0
    for t in d.get("trades", []):
        vol = float(t.get("volume", "0"))
        funds = float(t.get("funds", "0"))  # KRW
        total_volume += vol
        total_funds += funds
    if side == "bid":     # 매수
        return total_funds, total_volume, paid_fee
    else:                 # 매도
        return total_funds, total_volume, paid_fee

# ----------------- Binance Futures -----------------
def _bn_keys():
    keys = load_api_keys()
    return keys.get('binance_key', ""), keys.get('binance_secret', "")

def _bn_signed_url(path: str, params: dict) -> tuple[str, dict]:
    api_key, api_secret = _bn_keys()
    if "timestamp" not in params:
        params["timestamp"] = int(time.time() * 1000)
    qs = urlencode({k: v for k, v in params.items() if v is not None})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com{path}?{qs}&signature={sig}"
    headers = {"X-MBX-APIKEY": api_key}
    return url, headers

def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        url, headers = _bn_signed_url("/fapi/v1/leverage", {"symbol": symbol, "leverage": leverage})
        res = requests.post(url, headers=headers, timeout=10)
        log(f"[바이낸스 레버리지 설정] {symbol} x{leverage} → {res.text}")
        return res.status_code == 200
    except Exception as e:
        log(f"[레버리지 설정 오류] {e}")
        return False

def set_binance_isolated(symbol: str) -> bool:
    try:
        url, headers = _bn_signed_url("/fapi/v1/marginType", {"symbol": symbol, "marginType": "ISOLATED"})
        res = requests.post(url, headers=headers, timeout=10)
        if res.status_code == 200:
            log(f"[마진모드] {symbol} 격리(ISOLATED) 설정 완료")
            return True
        else:
            log(f"[마진모드 응답] {res.text} (환경상 격리 변경 불가일 수 있음)")
            return True
    except Exception as e:
        log(f"[마진모드 설정 오류] {e}")
        return False

def get_binance_leverage(symbol: str = "BTCUSDT") -> int:
    api_key, api_secret = _bn_keys()
    ts = int(time.time() * 1000)
    qs = urlencode({"timestamp": ts})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{qs}&signature={sig}"
    res = requests.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10).json()
    for p in res:
        if p.get("symbol") == "BTCUSDT":
            try:
                return int(float(p.get("leverage", "0")))
            except Exception:
                return 0
    return 0

def ensure_binance_margin_and_leverage(symbol: str = "BTCUSDT", leverage: int = 3) -> None:
    set_binance_isolated(symbol)
    for _ in range(3):
        set_binance_leverage(symbol, leverage)
        time.sleep(0.3)
        cur = get_binance_leverage(symbol)
        log(f"[레버리지 확인] 현재={cur}, 목표={leverage}")
        if cur == leverage:
            return
    log(f"[경고] 레버리지 {leverage} 설정 확인 실패")

def get_binance_available_usdt() -> float:
    api_key, api_secret = _bn_keys()
    ts = int(time.time() * 1000)
    qs = urlencode({"timestamp": ts})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/balance?{qs}&signature={sig}"
    res = requests.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10).json()
    for a in res:
        if a.get("asset") == "USDT":
            try:
                return float(a.get("availableBalance", "0"))
            except Exception:
                pass
    return 0.0

def get_binance_position_qty() -> float:
    api_key, api_secret = _bn_keys()
    ts = int(time.time() * 1000)
    qs = urlencode({"timestamp": ts})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{qs}&signature={sig}"
    res = requests.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10).json()
    for p in res:
        if p["symbol"] == "BTCUSDT":
            return float(p["positionAmt"])   # 숏이면 음수
    return 0.0

def binance_order(side: str, quantity: float, reduce_only: bool=False) -> Tuple[bool, Optional[int]]:
    """
    반환: (성공여부, orderId)
    """
    try:
        qty = floor_step(float(quantity), 0.001)
        if qty <= 0:
            return True, None
        params = {
            "symbol": "BTCUSDT",
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(qty, ".3f"),
            "reduceOnly": "true" if reduce_only else None
        }
        url, headers = _bn_signed_url("/fapi/v1/order", params)
        res = requests.post(url, headers=headers, timeout=10)
        try:
            data = res.json()
        except Exception:
            data = {"raw": res.text}
        log(f"[바이낸스 주문] {side.upper()} qty={format(qty,'.3f')} reduceOnly={reduce_only} → {json.dumps(data, ensure_ascii=False)}")
        if res.status_code == 200 and isinstance(data, dict) and "orderId" in data:
            return True, int(data["orderId"])
        return False, None
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False, None

def binance_user_trades(order_id: int) -> List[dict]:
    api_key, api_secret = _bn_keys()
    params = {"symbol": "BTCUSDT", "orderId": order_id, "timestamp": int(time.time()*1000)}
    url, headers = _bn_signed_url("/fapi/v1/userTrades", params)
    res = requests.get(url, headers=headers, timeout=10)
    return res.json() if res.status_code == 200 else []

def summarize_binance_order(order_id: int) -> Tuple[float, float, float]:
    """
    orderId 기준 체결 합산
    반환: (총수량BTC, 체결가가중평균USDT, 수수료합USDT)
    """
    fills = binance_user_trades(order_id)
    qty = 0.0
    quote = 0.0
    fee = 0.0
    for f in fills:
        q = float(f.get("qty", "0"))
        p = float(f.get("price", "0"))
        commission = float(f.get("commission", "0"))
        qty += q
        quote += q * p
        fee += commission  # commissionAsset 보통 USDT
    avg = (quote / qty) if qty > 0 else 0.0
    return qty, avg, fee

def get_binance_size_usdt(mark_price: float | None = None) -> float:
    if mark_price is None:
        mark_price = get_binance_price()
    qty = abs(get_binance_position_qty())
    return round(qty * mark_price, 6)

def adjust_binance_size_to_target(target_size_usdt: float, ref_price: float, tol_usdt: float = 5.0) -> None:
    def cur_size():
        qty = abs(get_binance_position_qty())
        return qty * ref_price
    for _ in range(4):
        now = cur_size()
        diff = target_size_usdt - now
        if abs(diff) <= tol_usdt:
            log(f"[사이즈 OK] 목표 {round(target_size_usdt,2)}USDT, 현재 {round(now,2)}USDT (±{tol_usdt})")
            return
        qty = floor_step(abs(diff) / ref_price, 0.001)
        if qty <= 0:
            return
        if diff > 0:
            ok, oid = binance_order("sell", qty, reduce_only=False)
            if oid: current_cycle["binance_sell_id"] = oid  # 증분도 마지막 oid 보관
            log(f"[보정] 모자람 {round(diff,2)}USDT → 추가 SELL {qty} BTC")
        else:
            ok, oid = binance_order("buy", qty, reduce_only=True)
            if oid: current_cycle["binance_buy_id"] = oid
            log(f"[보정] 초과 {round(-diff,2)}USDT → BUY(RO) {qty} BTC")
        time.sleep(0.4)

# --------- 전량 청산 루틴 ----------
def full_exit_with_retries(max_retries: int = 6, retry_delay: float = 1.2) -> Tuple[bool, Optional[str], Optional[int]]:
    """
    반환: (완료여부, upbit_sell_uuid, binance_buy_orderId)
    """
    up_uuid = None
    bn_oid = None
    for attempt in range(1, max_retries + 1):
        upbit_bal = round(get_upbit_btc_balance(), 6)
        pos_amt   = get_binance_position_qty()     # 숏이면 음수
        need_up   = upbit_bal > 0
        need_bin  = pos_amt < -1e-6

        log(f"[청산 체크#{attempt}] upbit={upbit_bal} BTC, binance_pos={pos_amt}")

        ok_u = True
        if need_up:
            ok_u, up_uuid = upbit_order("sell", 0, upbit_bal)
            if ok_u and up_uuid:
                current_cycle["upbit_sell_uuid"] = up_uuid

        ok_b = True
        if need_bin:
            ok_b, bn_oid = binance_order("buy", abs(pos_amt), reduce_only=True)
            if ok_b and bn_oid:
                current_cycle["binance_buy_id"] = bn_oid

        time.sleep(retry_delay)

        up2 = round(get_upbit_btc_balance(), 6)
        pos2 = get_binance_position_qty()
        closed = (up2 < 1e-6) and (-1e-6 <= pos2 <= 1e-6)

        log(f"[청산 재확인#{attempt}] upbit={up2}, binance_pos={pos2}, result={'OK' if closed else 'RETRY'}")

        if ok_u and ok_b and closed:
            return True, up_uuid, bn_oid
    return False, up_uuid, bn_oid

# ------------------ PnL 계산 ------------------
def compute_cycle_pnl_and_log(amount_krw: float, fx_for_exit: float) -> float:
    """
    현재 current_cycle에 저장된 체결정보로 정확 손익 계산 및 로그
    반환: 총 손익(KRW)
    """
    # 업비트 측 체결 요약(있으면 갱신)
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

    # 바이낸스 측 체결 요약
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

    # 선물 실현손익(USDT): 숏 → (입가격 - 출가격) * 수량 - 수수료
    qty_close = min(current_cycle["binance_entry_qty"], current_cycle["binance_exit_qty"])
    fut_pnl_usdt = (current_cycle["binance_entry_avg"] - current_cycle["binance_exit_avg"]) * qty_close - current_cycle["binance_fee_usdt"]

    total_pnl_krw = upbit_pnl + fut_pnl_usdt * fx_for_exit

    # 수익률(%)
    ret_pct = (total_pnl_krw / amount_krw * 100.0) if amount_krw > 0 else 0.0

    log(f"[실현손익] {ret_pct:+.2f}% | {int(round(total_pnl_krw)):+,} KRW "
        f"(Upbit {int(round(upbit_pnl)):+,} KRW, Binance {fut_pnl_usdt:+.3f} USDT @ FX={fx_for_exit:.2f})")

    return total_pnl_krw

def reset_cycle():
    for k in list(current_cycle.keys()):
        if isinstance(current_cycle[k], (int, float)):
            current_cycle[k] = 0 if isinstance(current_cycle[k], int) else 0.0
        else:
            current_cycle[k] = None

# ------------------ main loop ------------------
def run_strategy_thread(config: Dict[str, Any]) -> None:
    global running, position_state, trade_count, profit_krw, total_pnl
    global entry_info, entry_kimp_value, last_exit_ts, exiting

    target_kimp = float(config['target_kimp'])  # 예: -0.8
    exit_kimp   = float(config['exit_kimp'])    # 예: -0.5
    tolerance   = float(config['tolerance'])
    amount_krw  = float(config['amount_krw'])   # 업비트 100만원, 바이낸스 notional도 동일
    leverage    = 3

    ensure_binance_margin_and_leverage("BTCUSDT", leverage)

    while running:
        try:
            # >>> 캐시에서 초고속 스냅샷 읽기
            kimp_view, up, bi, fx = get_market_snapshot()
            kimp = kimp_view  # 이미 반올림됨(표시용). 비교는 그대로 사용해도 충분함.

            # 상태 보정
            if position_state == "entered":
                if get_upbit_btc_balance() < 1e-6 and abs(get_binance_position_qty()) < 1e-6:
                    position_state = "neutral"
                    entry_info["upbit_qty"] = 0.0
                    entry_info["binance_qty"] = 0.0
                    entry_kimp_value = None
                    log("[상태 보정] 실측 0/0 → neutral")

            # ENTRY
            if position_state == "neutral":
                dir_ok  = (kimp <= target_kimp) if target_kimp < 0 else (kimp >= target_kimp)
                near_ok = (kimp <= target_kimp + tolerance) if target_kimp < 0 else (kimp >= target_kimp - tolerance)
                cool_ok = (time.time() - last_exit_ts) > EXIT_COOLDOWN_SEC

                log(f"[체크] kimp={kimp_view}% target={target_kimp} tol={tolerance} dir_ok={dir_ok} near_ok={near_ok} cool_ok={cool_ok} state={position_state}")

                if dir_ok and near_ok and cool_ok:
                    reset_cycle()  # 새 사이클 시작
                    # 목표 사이즈/증거금
                    target_size_usdt = amount_krw / fx
                    need_margin_usdt = target_size_usdt / leverage
                    avail_usdt = get_binance_available_usdt()
                    log(f"[검사] 목표사이즈≈{round(target_size_usdt,2)}USDT, 필요증거금≈{round(need_margin_usdt,2)}USDT, 가용≈{round(avail_usdt,2)}USDT")

                    if avail_usdt + 1e-6 < need_margin_usdt:
                        log("[진입 중단] 바이낸스 가용 USDT 부족")
                        time.sleep(1)
                        continue

                    # 업비트 매수(시장가-금액지정)
                    upbit_qty = floor_step(amount_krw / up, 0.001)
                    ok_u, u_uuid = upbit_order("buy", amount_krw, upbit_qty)
                    if ok_u and u_uuid:
                        current_cycle["upbit_buy_uuid"] = u_uuid

                    # 바이낸스 숏 진입
                    first_qty = floor_step(target_size_usdt / bi, 0.001)
                    ok_b, b_oid = binance_order("sell", first_qty, reduce_only=False)
                    if ok_b and b_oid:
                        current_cycle["binance_sell_id"] = b_oid

                    # 사이즈 보정
                    time.sleep(0.6)
                    adjust_binance_size_to_target(target_size_usdt, ref_price=bi, tol_usdt=5.0)

                    if ok_u and ok_b:
                        entry_info["upbit_qty"] = upbit_qty
                        entry_info["binance_qty"] = floor_step(target_size_usdt / bi, 0.001)
                        position_state = "entered"
                        entry_kimp_value = round(kimp, 4)
                        log(f"[진입 성공] 업비트 +{upbit_qty}BTC(≈{int(amount_krw)}KRW) / "
                            f"바이낸스 숏 ≈{round(target_size_usdt,2)}USDT (x{leverage}) @ 김프 {kimp_view}%")
                    else:
                        log("[진입 실패] 한쪽 체결 실패")

            # EXIT
            elif position_state == "entered":
                if exiting:
                    time.sleep(0.3)
                    continue

                # 청산 직전 재계산 (캐시 스냅샷)
                kimp_now_view, up2, bi2, fx2 = get_market_snapshot()
                kimp_now = kimp_now_view

                crossed = False
                if entry_kimp_value is not None:
                    crossed = (kimp_now >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp_now <= exit_kimp)

                if crossed:
                    log(f"[청산 트리거] entry={entry_kimp_value}, exit_target={exit_kimp}, kimp_now={kimp_now_view}%")
                    exiting = True
                    try:
                        done, up_uuid, bn_oid = full_exit_with_retries(max_retries=6, retry_delay=1.2)
                        if done:
                            trade_count += 1
                            position_state = "neutral"
                            if up_uuid: current_cycle["upbit_sell_uuid"] = up_uuid
                            if bn_oid:  current_cycle["binance_buy_id"] = bn_oid

                            # === 실현손익 계산 & 로그 ===
                            pnl_krw = compute_cycle_pnl_and_log(amount_krw=amount_krw, fx_for_exit=fx2)
                            profit_krw += pnl_krw

                            delta = 0.0 if entry_kimp_value is None else round(abs(exit_kimp - entry_kimp_value), 3)
                            log(f"[청산 성공] 김프 {kimp_now_view}% (진입 {entry_kimp_value} → 청산선 {exit_kimp}, Δ≈{delta}%)")

                            # 상태/사이클 리셋
                            entry_kimp_value = None
                            entry_info["upbit_qty"] = 0.0
                            entry_info["binance_qty"] = 0.0
                            last_exit_ts = time.time()
                            reset_cycle()
                        else:
                            log("[청산 미완료] 잔량 남음. 루프 재시도")
                    finally:
                        exiting = False

        except Exception as e:
            log(f"[에러] {e}")

        time.sleep(1)

def start_strategy(config: Dict[str, Any]) -> None:
    global running
    if not running:
        ensure_market_updater()  # <<< 초고속 캐시 스레드 보장
        running = True
        threading.Thread(target=run_strategy_thread, args=(config,), daemon=True).start()
        log("[전략 시작]")

def stop_strategy() -> None:
    global running, entry_kimp_value, exiting
    running = False
    entry_kimp_value = None
    exiting = False
    log("[전략 중지]")

def get_strategy_status() -> Dict[str, Any]:
    return {
        "running": running,
        "position_state": position_state,
        "profit_krw": round(profit_krw),
        "total_pnl": total_pnl,
        "trade_count": trade_count,
        "logs": logs,
        "entry_info": entry_info
    }

__all__ = ["start_strategy", "stop_strategy", "get_strategy_status"]
