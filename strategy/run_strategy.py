from __future__ import annotations
import time, threading, requests, hmac, hashlib, uuid, jwt
from urllib.parse import urlencode
from typing import Any, Dict
from api.api_key import load_api_keys

# ===== 전역 상태 =====
running: bool = False
position_state: str = "neutral"      # neutral / entered
trade_count: int = 0
profit_krw: float = 0.0
total_pnl: float = 0.0
logs: list[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}
entry_kimp_value: float | None = None

# ✅ 청산 직후 즉시 재진입 방지
last_exit_ts: float = 0.0
EXIT_COOLDOWN_SEC: float = 3.0  # 필요 없으면 0으로

# --------------------- utils ---------------------
def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]")
    logs.append(f"{ts} {msg}")
    if len(logs) > 300:
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
        s = text.find('data-last-price="') + len('data-last-price="')
        e = text.find('"', s)
        return float(text[s:e].replace(',', ''))
    except Exception:
        return 1300.0

def floor_step(x: float, step: float) -> float:
    return (int(float(x) / step)) * step  # 거래소 최소 스텝 보정

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

def upbit_order(side: str, price_krw: float, volume_btc: float) -> bool:
    # buy: KRW 금액 시장가 / sell: BTC 수량 시장가
    if side == 'buy':
        query = {"market": "KRW-BTC", "side": "bid", "ord_type": "price", "price": str(int(price_krw))}
    else:
        query = {"market": "KRW-BTC", "side": "ask", "ord_type": "market", "volume": format(float(volume_btc), ".4f")}
    headers = upbit_auth_headers(True, query)
    res = requests.post("https://api.upbit.com/v1/orders", params=query, headers=headers, timeout=10)
    log(f"[업비트 주문] {side.upper()} vol={query.get('volume','')} price={query.get('price','')} → {res.text}")
    return res.status_code == 201

# ----------------- Binance Futures -----------------
def set_binance_leverage(symbol: str, leverage: int) -> bool:
    try:
        keys = load_api_keys()
        api_key, api_secret = keys.get('binance_key', ""), keys.get('binance_secret', "")
        ts = int(time.time() * 1000)
        params = {"symbol": symbol, "leverage": leverage, "timestamp": ts}
        qs = urlencode(params)
        sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"https://fapi.binance.com/fapi/v1/leverage?{qs}&signature={sig}"
        res = requests.post(url, headers={"X-MBX-APIKEY": api_key}, timeout=10)
        log(f"[바이낸스 레버리지 설정] {symbol} x{leverage} → {res.text}")
        return res.status_code == 200
    except Exception as e:
        log(f"[레버리지 설정 오류] {e}")
        return False

def get_binance_position_qty() -> float:
    keys = load_api_keys()
    api_key, api_secret = keys.get('binance_key', ""), keys.get('binance_secret', "")
    ts = int(time.time() * 1000)
    qs = urlencode({"timestamp": ts})
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    url = f"https://fapi.binance.com/fapi/v2/positionRisk?{qs}&signature={sig}"
    res = requests.get(url, headers={"X-MBX-APIKEY": api_key}, timeout=10).json()
    for p in res:
        if p["symbol"] == "BTCUSDT":
            return float(p["positionAmt"])   # 숏이면 음수
    return 0.0

def binance_order(side: str, quantity: float, reduce_only: bool=False) -> bool:
    try:
        qty = floor_step(float(quantity), 0.001)  # BTCUSDT 최소 수량단위
        if qty <= 0:
            return True
        keys = load_api_keys()
        api_key, api_secret = keys.get('binance_key', ""), keys.get('binance_secret', "")
        ts = int(time.time() * 1000)
        params = {
            "symbol": "BTCUSDT",
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(qty, ".3f"),
            "timestamp": ts
        }
        if reduce_only:
            params["reduceOnly"] = "true"  # 롱 전환 금지, 포지션 축소만
        qs = urlencode(params)
        sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
        url = f"https://fapi.binance.com/fapi/v1/order?{qs}&signature={sig}"
        res = requests.post(url, headers={"X-MBX-APIKEY": api_key}, timeout=10)
        log(f"[바이낸스 주문] {side.upper()} qty={params['quantity']} reduceOnly={reduce_only} → {res.text}")
        return res.status_code == 200
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False

# --------- 공통: 양쪽 전량 청산 루틴 ----------
def full_exit_with_retries(max_retries: int = 6, retry_delay: float = 1.2) -> bool:
    """
    업비트 BTC 전량 매도 + 바이낸스 숏 포지션 전량 커버(BUY reduceOnly)
    수량이 서로 달라도 실측 기준으로 0/0 될 때까지 재시도.
    """
    for attempt in range(1, max_retries + 1):
        upbit_bal = round(get_upbit_btc_balance(), 6)
        pos_amt   = get_binance_position_qty()     # 숏이면 음수
        need_up   = upbit_bal > 0
        need_bin  = pos_amt < -1e-6                # 숏이 남아 있으면 커버 필요

        log(f"[청산 체크#{attempt}] upbit={upbit_bal} BTC, binance_pos={pos_amt}")

        ok_u = True
        if need_up:
            ok_u = upbit_order("sell", 0, upbit_bal)

        ok_b = True
        if need_bin:
            ok_b = binance_order("buy", abs(pos_amt), reduce_only=True)  # 숏만 닫기, 롱 금지

        time.sleep(retry_delay)

        up2 = round(get_upbit_btc_balance(), 6)
        pos2 = get_binance_position_qty()
        closed = (up2 < 1e-6) and (-1e-6 <= pos2 <= 1e-6)

        log(f"[청산 재확인#{attempt}] upbit={up2}, binance_pos={pos2}, result={'OK' if closed else 'RETRY'}")

        if ok_u and ok_b and closed:
            return True
    return False

# ------------------ main loop ------------------
def run_strategy_thread(config: Dict[str, Any]) -> None:
    global running, position_state, trade_count, profit_krw, total_pnl
    global entry_info, entry_kimp_value, last_exit_ts

    target_kimp = float(config['target_kimp'])
    exit_kimp   = float(config['exit_kimp'])
    tolerance   = float(config['tolerance'])
    amount_krw  = float(config['amount_krw'])
    leverage    = 3  # 요구사항: 항상 레버리지 3배 사용

    set_binance_leverage("BTCUSDT", leverage)

    while running:
        try:
            up = get_upbit_price()
            bi = get_binance_price()
            fx = get_usdkrw()

            kimp = (up - bi * fx) / (bi * fx) * 100.0
            kimp_view = round(kimp, 2)

            # ---- 상태 자동 보정: 실측 0/0이면 neutral ----
            if position_state == "entered":
                if get_upbit_btc_balance() < 1e-6 and abs(get_binance_position_qty()) < 1e-6:
                    position_state = "neutral"
                    entry_info["upbit_qty"] = 0.0
                    entry_info["binance_qty"] = 0.0
                    entry_kimp_value = None
                    log("[상태 보정] 실측 0/0 → neutral")

            # -------- ENTRY: Upbit Long + Binance Short --------
            if position_state == "neutral":
                dir_ok  = (kimp <= target_kimp) if target_kimp < 0 else (kimp >= target_kimp)
                near_ok = abs(kimp - target_kimp) <= tolerance     # ✅ 오차는 진입에만
                cool_ok = (time.time() - last_exit_ts) > EXIT_COOLDOWN_SEC

                if dir_ok and near_ok and cool_ok:
                    upbit_qty   = floor_step(amount_krw / up, 0.001)    # 업비트 매수 수량
                    binance_qty = upbit_qty                              # 레버리지와 무관, 수량 동일
                    ok_u = upbit_order("buy", amount_krw, upbit_qty)
                    ok_b = binance_order("sell", binance_qty, reduce_only=False)  # 숏 오픈
                    if ok_u and ok_b:
                        entry_info["upbit_qty"] = upbit_qty
                        entry_info["binance_qty"] = binance_qty
                        position_state = "entered"
                        entry_kimp_value = round(kimp, 4)
                        log(f"[진입 성공] 업비트 +{upbit_qty} / 바이낸스 -{binance_qty} @ 김프 {kimp_view}%")
                    else:
                        log("[진입 실패] 한쪽 체결 실패. 다음 루프에서 재시도")

            # -------- EXIT: 설정한 청산 김프 도달 시 전량 청산 --------
            elif position_state == "entered":
                if entry_kimp_value is not None:
                    # entry < exit → 상향 돌파 / entry > exit → 하향 돌파
                    crossed = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)
                else:
                    crossed = False  # 기록 없으면 보수적으로 미청산

                if crossed:
                    log(f"[청산 트리거] entry={entry_kimp_value}, exit_target={exit_kimp}, 현재 김프={kimp_view}%")
                    if full_exit_with_retries(max_retries=6, retry_delay=1.2):
                        trade_count += 1
                        position_state = "neutral"
                        entry_info["upbit_qty"] = 0.0
                        entry_info["binance_qty"] = 0.0
                        delta = 0.0 if entry_kimp_value is None else round(abs(exit_kimp - entry_kimp_value), 3)
                        log(f"[청산 성공] {kimp_view}% (진입 {entry_kimp_value} → 청산선 {exit_kimp}, Δ≈{delta}%)")
                        entry_kimp_value = None
                        last_exit_ts = time.time()     # ✅ 즉시 재진입 방지
                    else:
                        log("[청산 미완료] 잔량 남음. 루프에서 재시도")

        except Exception as e:
            log(f"[에러] {e}")

        time.sleep(1)

def start_strategy(config: Dict[str, Any]) -> None:
    global running
    if not running:
        running = True
        threading.Thread(target=run_strategy_thread, args=(config,), daemon=True).start()
        log("[전략 시작]")

def stop_strategy() -> None:
    global running, entry_kimp_value
    running = False
    entry_kimp_value = None
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
