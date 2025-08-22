from __future__ import annotations
import os, time, threading, requests, hmac, hashlib, uuid, jwt
from urllib.parse import urlencode
from typing import Any, Dict, Tuple, Optional

# ===== 전역 상태 =====
running: bool = False
position_state: str = "neutral"      # neutral / entered
trade_count: int = 0
profit_krw: float = 0.0
total_pnl: float = 0.0
logs: list[str] = []
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}
entry_kimp_value: float | None = None
last_error: str | None = None

# 거래소 스텝/상수
UPBIT_QTY_STEP = 0.0001
BINANCE_QTY_STEP = 0.001
BINANCE_SYMBOL = "BTCUSDT"
RECV_WINDOW = 5000  # ms
MAX_RETRY_ON_1021 = 1  # -1021(시간오류) 감지 시 자동 재시도 횟수

# 테스트넷/메인넷 자동 분기
BINANCE_TESTNET = str(os.getenv("BINANCE_TESTNET", "false")).lower() in ("1", "true", "yes")
BASE_FAPI = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"

# Binance 시간 동기화(밀리초)
_binance_time_offset_ms: int = 0

# --------------------- utils ---------------------
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
    # 선물 가격 티커(테스트넷/메인넷 공통 BASE_FAPI 사용)
    r = requests.get(f"{BASE_FAPI}/fapi/v1/ticker/price", params={"symbol": BINANCE_SYMBOL}, timeout=10)
    r.raise_for_status()
    return float(r.json()['price'])

def get_usdkrw() -> float:
    # 우선 KRW-USDT 사용(USDT≈USD 가정), 실패 시 1300
    try:
        r = requests.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=5)
        return float(r.json()[0]["trade_price"])
    except Exception:
        return 1300.0

# ----------------- Upbit -----------------
def _upbit_headers(query: dict | None = None) -> dict:
    from api.api_key import load_api_keys
    keys = load_api_keys()
    access_key, secret_key = keys.get('upbit_key', ""), keys.get('upbit_secret', "")
    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    if query is not None:
        q = urlencode(query).encode()
        m = hashlib.sha512()
        m.update(q)
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

def upbit_market_buy_by_krw(amount_krw: float) -> tuple[bool, float, str]:
    before = get_upbit_btc_balance()
    q = {"market":"KRW-BTC","side":"bid","ord_type":"price","price":str(int(amount_krw))}
    r = requests.post("https://api.upbit.com/v1/orders", params=q, headers=_upbit_headers(q), timeout=10)
    text = r.text
    log(f"[업비트 매수] KRW={int(amount_krw)} → {text}")
    if r.status_code != 201:
        return False, 0.0, text

    got = 0.0
    # 최대 3초 폴링하여 체결 수량 파악
    for _ in range(6):
        time.sleep(0.5)
        after = get_upbit_btc_balance()
        got = max(0.0, round(after - before, 8))
        if got > 0:
            break
    return (got > 0), got, text

def upbit_market_sell_btc(volume_btc: float) -> tuple[bool, str]:
    vol = format(floor_step(volume_btc, UPBIT_QTY_STEP), ".8f")
    q = {"market":"KRW-BTC","side":"ask","ord_type":"market","volume":vol}
    r = requests.post("https://api.upbit.com/v1/orders", params=q, headers=_upbit_headers(q), timeout=10)
    text = r.text
    log(f"[업비트 매도] vol={vol} → {text}")
    return (r.status_code == 201), text

# ----------------- Binance Futures (공통) -----------------
def _binance_keys() -> Tuple[str, str]:
    from api.api_key import load_api_keys
    k = load_api_keys()
    return k.get('binance_key', ""), k.get('binance_secret', "")

def _bn_now_ms() -> int:
    return int(time.time()*1000) + _binance_time_offset_ms

def _bn_headers() -> dict:
    api_key, _ = _binance_keys()
    return {
        "X-MBX-APIKEY": api_key,
        "Content-Type": "application/x-www-form-urlencoded"
    }

def _bn_sign(params: Dict[str, Any]) -> str:
    """params -> querystring(signature 포함)"""
    _, sec = _binance_keys()
    qs = urlencode(params, doseq=True)
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs + "&signature=" + sig

def _is_ts_error(resp_text: str) -> bool:
    # -1021 timestamp 관련 에러 탐지
    t = (resp_text or "").lower()
    return "-1021" in t or "timestamp" in t and ("ahead" in t or "recvwindow" in t or "server time" in t)

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

# ----------------- Binance Futures (기능) -----------------
def ensure_one_way_mode() -> bool:
    """헷지모드 OFF(원웨이). dualSidePosition=true 상태면 positionSide 미지정 주문이 실패."""
    try:
        r = _bn_signed_post("/fapi/v1/positionSide/dual", {"dualSidePosition": "false"})
        log(f"[바이낸스 모드] 원웨이 설정 → status={r.status_code}, body={r.text[:160]}")
        if r.status_code == 200:
            return True
        if r.status_code in (401, 403) or '"code":-2015' in r.text:
            # 키 권한/선물 미활성/테스트넷-메인넷 불일치
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
        log(f"[마진모드] ISOLATED={isolated} → status={r.status_code}, body={r.text[:160]}")
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
        log(f"[레버리지 설정] {symbol} x{leverage} → status={r.status_code}, body={r.text[:160]}")
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
            log(f"[포지션 조회 실패] status={r.status_code}, body={r.text[:200]}")
            return 0.0
        for p in r.json():
            if p.get("symbol") == BINANCE_SYMBOL:
                return float(p.get("positionAmt", 0.0))  # 숏<0, 롱>0
    except Exception as e:
        log(f"[포지션 조회 예외] {e}")
    return 0.0

def get_binance_futures_usdt_balance() -> Dict[str, float] | Dict[str, str]:
    """USDT-M 선물 잔고(available 포함)"""
    try:
        r = _bn_signed_get("/fapi/v2/balance")
        if r.status_code != 200:
            log(f"[잔고 조회 실패] status={r.status_code}, body={r.text[:200]}")
            if '"code":-2015' in r.text:
                global last_error
                last_error = "binance_invalid_key_or_permission (-2015)"
            return {"error": r.text}
        balances = r.json()
        usdt = next((x for x in balances if x.get("asset") == "USDT"), None)
        if not usdt:
            return {"error": "USDT balance not found"}
        return {
            "total": float(usdt.get("balance", 0.0)),
            "available": float(usdt.get("availableBalance", 0.0))
        }
    except Exception as e:
        log(f"[잔고 조회 예외] {e}")
        return {"error": str(e)}

def binance_market_order(side: str, qty: float, reduce_only: bool=False) -> bool:
    try:
        q = floor_step(qty, BINANCE_QTY_STEP)
        if q <= 0:
            return True
        params = {
            "symbol": BINANCE_SYMBOL,
            "side": "SELL" if side == "sell" else "BUY",
            "type": "MARKET",
            "quantity": format(q, ".3f"),
        }
        if reduce_only:
            params["reduceOnly"] = "true"
        r = _bn_signed_post("/fapi/v1/order", params)
        log(f"[바이낸스 주문] {side.upper()} qty={params['quantity']} reduceOnly={reduce_only} → status={r.status_code}, body={r.text[:200]}")
        if r.status_code != 200 and ('"code":-2015' in r.text):
            global last_error
            last_error = "binance_invalid_key_or_permission (-2015)"
        return r.status_code == 200
    except Exception as e:
        log(f"[바이낸스 주문 예외] {e}")
        return False

# ------------------ 보조: 시장 스냅샷 ------------------
def get_market_snapshot() -> Dict[str, Any]:
    try:
        up = get_upbit_price()
        bi = get_binance_price()
        fx = get_usdkrw()
        kimp = (up - bi * fx) / (bi * fx) * 100.0
        return {"upbit_price": up, "binance_price": bi, "usdkrw": fx, "kimp": kimp}
    except Exception as e:
        return {"error": str(e)}

# ------------------ 핵심: “반드시 진입/반드시 청산” 루틴 ------------------
def must_enter(amount_krw: float, poll_sec: float=0.5, leverage:int=3, tol_usdt:float=5.0) -> Tuple[float, float]:
    """
    업비트 1배 노출 = 바이낸스 명목가(USDT) 동일 맞춤.
    - 목표USDT = amount_krw / fx
    - 바이낸스 수량(BTC) = 목표USDT / binance_price   (수량을 레버리지로 나누지 않음)
    - 꼭 필요한 증거금 = 목표USDT / leverage  (가용 USDT 체크)
    - 체결 후 현재 명목가를 ±tol_usdt 이내가 될 때까지 자동 보정
    반환: (업비트 체결수량 BTC, 바이낸스 최종 포지션수량 BTC)
    """
    # 스냅샷 확보
    snap = get_market_snapshot()
    if "error" in snap:
        log(f"[진입 중단] 스냅샷 오류: {snap['error']}")
        return 0.0, 0.0
    up = snap["upbit_price"]; bi = snap["binance_price"]; fx = snap["usdkrw"]

    target_size_usdt = amount_krw / fx
    need_margin_usdt = target_size_usdt / max(1, leverage)

    # 가용 증거금 검증
    bal = get_binance_futures_usdt_balance()
    if isinstance(bal, dict) and "available" in bal:
        avail = float(bal["available"])
        if avail + 1e-6 < need_margin_usdt:
            log(f"[진입 중단] 가용USDT 부족 avail={avail:.2f} < need={need_margin_usdt:.2f}")
            return 0.0, 0.0

    # 1) 업비트 금액 시장가 매수 → 실체결 수량 확보
    ok_u, got, _ = upbit_market_buy_by_krw(amount_krw)
    if not ok_u or got <= 0:
        log("[진입] 업비트 체결 실패/수량 0")
        return 0.0, 0.0
    upbit_qty = floor_step(got, UPBIT_QTY_STEP)
    log(f"[진입] 업비트 체결수량: {upbit_qty}")

    # 2) 바이낸스 숏: 목표 USDT 기준으로 수량 산정
    target_qty = floor_step(target_size_usdt / bi, BINANCE_QTY_STEP)
    if target_qty <= 0:
        log("[진입 중단] 산정 수량 0")
        return 0.0, 0.0

    # 최초 진입
    _ = binance_market_order("sell", target_qty, reduce_only=False)
    time.sleep(poll_sec)

    # 명목가 보정 루프(±tol_usdt)
    for _ in range(12):
        pos = abs(get_binance_position_qty())
        cur_usdt = pos * bi
        diff = target_size_usdt - cur_usdt

        if abs(diff) <= tol_usdt:
            log(f"[진입] 바이낸스 목표USDT 근접 OK (target≈{target_size_usdt:.2f}, now≈{cur_usdt:.2f}, tol={tol_usdt})")
            return upbit_qty, floor_step(pos, BINANCE_QTY_STEP)

        # 모자람 → 추가 SELL, 초과 → BUY(reduceOnly)
        adj_qty = floor_step(abs(diff) / bi, BINANCE_QTY_STEP)
        if adj_qty <= 0:
            break
        if diff > 0:
            binance_market_order("sell", adj_qty, reduce_only=False)
            log(f"[보정] 모자람 {diff:.2f}USDT → SELL {adj_qty} BTC")
        else:
            binance_market_order("buy", adj_qty, reduce_only=True)
            log(f"[보정] 초과 {(-diff):.2f}USDT → BUY(RO) {adj_qty} BTC")

        time.sleep(poll_sec)
        if last_error and "binance_invalid_key_or_permission" in last_error:
            break

    # 최종 확인
    final_pos = abs(get_binance_position_qty())
    return upbit_qty, floor_step(final_pos, BINANCE_QTY_STEP)

def must_exit(poll_sec: float=0.5):
    # 1) 바이낸스 숏 청산
    for _ in range(20):
        pos = get_binance_position_qty()
        if pos >= -1e-6:
            log("[청산] 바이낸스 포지션=0 확인")
            break
        binance_market_order("buy", abs(pos), reduce_only=True)
        time.sleep(poll_sec)
    # 2) 업비트 현물 매도
    for _ in range(20):
        bal = get_upbit_btc_balance()
        if bal < 1e-6:
            log("[청산] 업비트 잔고=0 확인")
            break
        upbit_market_sell_btc(bal)
        time.sleep(poll_sec)

# ------------------ main loop ------------------
def run_strategy_thread(config: Dict[str, Any]) -> None:
    global running, position_state, trade_count, profit_krw, total_pnl
    global entry_info, entry_kimp_value, last_error

    target_kimp = float(config['target_kimp'])
    exit_kimp   = float(config['exit_kimp'])
    tolerance   = float(config['tolerance'])
    amount_krw  = float(config['amount_krw'])

    # Binance 준비: 시간/모드/마진/레버리지
    sync_binance_server_time()
    ensure_one_way_mode()
    set_margin_mode(BINANCE_SYMBOL, isolated=True)
    set_binance_leverage(BINANCE_SYMBOL, 3)  # 3배 (수량엔 영향 없음)

    while running:
        try:
            # 잔고 캐싱/표시용 조회(실패해도 전략 계속)
            _ = get_binance_futures_usdt_balance()

            snap = get_market_snapshot()
            if "error" in snap:
                last_error = snap["error"]
                time.sleep(1); continue

            up = snap["upbit_price"]; bi = snap["binance_price"]; fx = snap["usdkrw"]
            kimp = (up - bi * fx) / (bi * fx) * 100.0
            kimp_view = round(kimp, 2)
            last_error = None  # 직전 오류가 있었다면 정상화

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
                        log(f"[진입 완료] 업비트 +{uq}BTC / 바이낸스 -{bq}BTC (명목≈{amount_krw:.0f}KRW 동노출) @ {kimp_view}%")
                    else:
                        last_error = last_error or "binance_enter_failed"

            elif position_state == "entered":
                if entry_kimp_value is not None:
                    crossed = (kimp >= exit_kimp) if exit_kimp >= entry_kimp_value else (kimp <= exit_kimp)
                else:
                    crossed = False
                if crossed:
                    log(f"[청산 트리거] entry={entry_kimp_value}, exit={exit_kimp}, 현재={kimp_view}%")
                    must_exit()
                    trade_count += 1
                    position_state = "neutral"
                    entry_info.update({"upbit_qty":0.0,"binance_qty":0.0})
                    entry_kimp_value = None
                    log("[청산 완료] 양쪽 0 확인")

        except Exception as e:
            last_error = str(e)
            log(f"[에러] {e}")

        time.sleep(0.6)

# ------------------ 외부 인터페이스 ------------------
def start_strategy(config: Dict[str, Any]) -> None:
    global running, last_error
    if running:
        return
    # 키 기본 검증
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

def get_strategy_status() -> Dict[str, Any]:
    snap = get_market_snapshot()
    binance_bal = get_binance_futures_usdt_balance()
    return {
        "running": running,
        "position_state": position_state,
        "profit_krw": round(profit_krw),
        "total_pnl": total_pnl,
        "trade_count": trade_count,
        "entry_kimp_value": entry_kimp_value,
        "entry_info": entry_info,
        "last_error": last_error,
        "snapshot": snap,
        "balances": {
            "binance_futures_usdt": binance_bal
        },
        "logs": logs[-250:],
    }

__all__ = ["start_strategy", "stop_strategy", "get_strategy_status", "get_market_snapshot"]
