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
EXIT_COOLDOWN_SEC: float = 3.0

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

# ----------------- Binance Futures (helpers) -----------------
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
    """격리(ISOLATED) 마진 설정 (Multi-Assets 모드에선 거부될 수 있으므로 실패여도 계속 진행)"""
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
        if p.get("symbol") == symbol:
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
    """선물 지갑 가용 USDT"""
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

def binance_order(side: str, quantity: float, reduce_only: bool=False) -> bool:
    try:
        qty = floor_step(float(quantity), 0.001)  # BTCUSDT 최소 수량단위
        if qty <= 0:
            return True
        params = {
            "symbol": "BTCUSDT",
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(qty, ".3f"),
            "reduceOnly": "true" if reduce_only else None
        }
        url, headers = _bn_signed_url("/fapi/v1/order", params)
        res = requests.post(url, headers=headers, timeout=10)
        log(f"[바이낸스 주문] {side.upper()} qty={format(qty,'.3f')} reduceOnly={reduce_only} → {res.text}")
        return res.status_code == 200
    except Exception as e:
        log(f"[바이낸스 주문 오류] {e}")
        return False

def get_binance_size_usdt(mark_price: float | None = None) -> float:
    """현재 포지션의 절대 notional(USDT) 추정"""
    if mark_price is None:
        mark_price = get_binance_price()
    qty = abs(get_binance_position_qty())
    return round(qty * mark_price, 6)

def adjust_binance_size_to_target(target_size_usdt: float, ref_price: float, tol_usdt: float = 5.0) -> None:
    """
    목표 notional(USDT)로 맞출 때까지 보정.
    - 현재 사이즈 < 목표-오차 → 추가 SELL(숏 늘림)
    - 현재 사이즈 > 목표+오차 → BUY reduceOnly(숏 줄임)
    """
    def cur_size():
        qty = abs(get_binance_position_qty())
        return qty * ref_price

    for _ in range(4):  # 최대 4번 보정 시도
        now = cur_size()
        diff = target_size_usdt - now
        if abs(diff) <= tol_usdt:
            log(f"[사이즈 OK] 목표 {round(target_size_usdt,2)}USDT, 현재 {round(now,2)}USDT (±{tol_usdt})")
            return
        qty = floor_step(abs(diff) / ref_price, 0.001)
        if qty <= 0:
            return
        if diff > 0:
            binance_order("sell", qty, reduce_only=False)   # 모자라면 숏 추가
            log(f"[보정] 모자람 {round(diff,2)}USDT → 추가 SELL {qty} BTC")
        else:
            binance_order("buy", qty, reduce_only=True)     # 초과면 숏 일부 커버
            log(f"[보정] 초과 {round(-diff,2)}USDT → BUY(RO) {qty} BTC")
        time.sleep(0.4)

# --------- 공통: 양쪽 전량 청산 루틴 ----------
def full_exit_with_retries(max_retries: int = 6, retry_delay: float = 1.2) -> bool:
    for attempt in range(1, max_retries + 1):
        upbit_bal = round(get_upbit_btc_balance(), 6)
        pos_amt   = get_binance_position_qty()     # 숏이면 음수
        need_up   = upbit_bal > 0
        need_bin  = pos_amt < -1e-6

        log(f"[청산 체크#{attempt}] upbit={upbit_bal} BTC, binance_pos={pos_amt}")

        ok_u = True
        if need_up:
            ok_u = upbit_order("sell", 0, upbit_bal)

        ok_b = True
        if need_bin:
            ok_b = binance_order("buy", abs(pos_amt), reduce_only=True)

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
    amount_krw  = float(config['amount_krw'])   # ✅ 업비트 100만원, 바이낸스 "사이즈"도 100만원
    leverage    = 3                             # ✅ 항상 x3 (증거금=사이즈/3)

    # 마진모드/레버리지 보장 (Multi-Assets 환경이면 격리 전환은 무시되고, 레버리지만 확인됨)
    ensure_binance_margin_and_leverage("BTCUSDT", leverage)

    while running:
        try:
            up = get_upbit_price()
            bi = get_binance_price()
            fx = get_usdkrw()

            kimp = (up - bi * fx) / (bi * fx) * 100.0
            kimp_view = round(kimp, 2)

            # ---- 상태 자동 보정 ----
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
                # ✅ 변경: 목표보다 유리한 쪽도 허용 (tolerance는 버퍼)
                if target_kimp < 0:
                    near_ok = (kimp <= target_kimp + tolerance)
                else:
                    near_ok = (kimp >= target_kimp - tolerance)
                cool_ok = (time.time() - last_exit_ts) > EXIT_COOLDOWN_SEC

                # 디버그용 가시 로그
                log(f"[체크] kimp={kimp_view}% target={target_kimp} tol={tolerance} dir_ok={dir_ok} near_ok={near_ok} cool_ok={cool_ok} state={position_state}")

                if dir_ok and near_ok and cool_ok:
                    # === 목표 사이즈/증거금 계산 ===
                    target_size_usdt = amount_krw / fx                 # 예) 1,000,000 / 1300 ≈ 769 USDT (사이즈)
                    need_margin_usdt = target_size_usdt / leverage     # 증거금 = 사이즈/3
                    avail_usdt = get_binance_available_usdt()
                    log(f"[검사] 목표사이즈≈{round(target_size_usdt,2)}USDT, 필요증거금≈{round(need_margin_usdt,2)}USDT, 가용≈{round(avail_usdt,2)}USDT")

                    if avail_usdt + 1e-6 < need_margin_usdt:
                        log("[진입 중단] 바이낸스 가용 USDT가 필요 증거금보다 적습니다. (입금/전송 필요)")
                        time.sleep(1)
                        continue

                    # 업비트: 현물 100만원 매수
                    upbit_qty = floor_step(amount_krw / up, 0.001)
                    ok_u = upbit_order("buy", amount_krw, upbit_qty)

                    # 바이낸스: 초기 숏 진입(시장가)
                    first_qty = floor_step(target_size_usdt / bi, 0.001)
                    ok_b = binance_order("sell", first_qty, reduce_only=False)

                    # 체결 후 실측 사이즈 재확인 & 보정
                    time.sleep(0.6)
                    adjust_binance_size_to_target(target_size_usdt, ref_price=bi, tol_usdt=5.0)

                    if ok_u and ok_b:
                        entry_info["upbit_qty"] = upbit_qty
                        entry_info["binance_qty"] = floor_step(target_size_usdt / bi, 0.001)
                        position_state = "entered"
                        entry_kimp_value = round(kimp, 4)
                        log(f"[진입 성공] 업비트 +{upbit_qty}BTC(≈{int(amount_krw)}KRW) / "
                            f"바이낸스 숏 사이즈≈{round(target_size_usdt,2)}USDT (x{leverage}, 필요증거금≈{round(need_margin_usdt,2)}USDT) "
                            f"@ 김프 {kimp_view}%")
                    else:
                        log("[진입 실패] 한쪽 체결 실패. 다음 루프에서 재시도")

            # -------- EXIT: 설정 청산 김프 --------
            elif position_state == "entered":
                if entry_kimp_value is not None:
                    crossed = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)
                else:
                    crossed = False

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
                        last_exit_ts = time.time()
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
