from __future__ import annotations

import time
import uuid
import hmac
import jwt
import json
import hashlib
import threading
from typing import Any, Dict, Tuple, Optional
from collections import deque

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
    adapter = HTTPAdapter(max_retries=retry, pool_connections=32, pool_maxsize=64)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    return s


HTTP = _make_session()
TIMEOUT = (3, 7)  # (connect, read) seconds


# ===================== Global State =====================
MAX_LOGS = 1000

running_evt = threading.Event()        # ▶ 전략 루프 on/off
strategy_thread: Optional[threading.Thread] = None
state_lock = threading.Lock()          # ▶ 상태/로그 보호

position_state: str = "neutral"        # neutral | entered
trade_count: int = 0

# 가독용 누적 수익 (계산은 확장 시 추가)
profit_krw: float = 0.0
total_pnl: float = 0.0

# 가벼운 운영 메트릭
metrics: Dict[str, float] = {
    "loops": 0,
    "api_errors": 0,
    "orders_upbit": 0,
    "orders_binance": 0,
}

# 로그: 사람이 읽는 로그 + 구조화 로그(최근 N개)
logs = deque(maxlen=MAX_LOGS)
logs_json = deque(maxlen=MAX_LOGS)

entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}

# 트리거 보조
_last_action_ts: float = 0.0
COOLDOWN_SEC: float = 5.0

# 진입 당시 김프 저장(청산 방향 판정용)
entry_kimp_value: float | None = None

# 중복 청산 가드
exiting: bool = False


# ===================== Trading Safety Constants =====================
UPBIT_BTC_STEP = 0.0001           # 업비트 BTC 최소 수량 스텝(보수적)
UPBIT_MIN_KRW_ORDER = 5000        # 업비트 금액시장가 최소금액
DUST_BTC = UPBIT_BTC_STEP / 2.0   # 이하면 실질적으로 0으로 간주


# ===================== Logging =====================
def _ts() -> str:
    return time.strftime("%H:%M:%S")


def log(msg: str) -> None:
    """사람이 읽기 쉬운 단순 로그"""
    with state_lock:
        logs.append(f"[{_ts()}] {msg}")


def log_json(event: str, **fields) -> None:
    """구조화 로그: event + key/val"""
    payload = {"ts": _ts(), "event": event, **fields}
    with state_lock:
        logs_json.append(payload)
        # 사람이 보는 로그에도 간단 버전 남김
        logs.append(f"[{payload['ts']}] [{event}] {json.dumps(fields, ensure_ascii=False, sort_keys=True)}")


# ===================== Utils =====================
def floor_step(x: float, step: float) -> float:
    return (int(float(x) / step)) * step


def _safe_json(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


# ===================== 초고속 마켓 캐시 =====================
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

        # 업비트
        try:
            r1 = requests.get(upbit_url, timeout=(1, 2))
            r1.raise_for_status()
            up = float(r1.json()[0]["trade_price"])
            with MARKET.lock:
                MARKET.upbit = up
                MARKET.ts_upbit = now
        except Exception:
            pass

        # 바이낸스
        try:
            r2 = requests.get(binance_url, timeout=(1, 2))
            r2.raise_for_status()
            bi = float(r2.json()["price"])
            with MARKET.lock:
                MARKET.binance = bi
                MARKET.ts_binance = now
        except Exception:
            pass

        # 환율(비용 큼 → 드물게)
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
        bp = MARKET.binance
        fx = MARKET.usdkrw
        tsu, tsb, tsf = MARKET.ts_upbit, MARKET.ts_binance, MARKET.ts_fx

    # 스테일이면 폴백(부팅 직후 등)
    if (now - tsu > PRICE_STALE_SEC) or (now - tsb > PRICE_STALE_SEC) or (now - tsf > 120.0):
        up = get_upbit_price()
        bp = get_binance_price()
        fx = get_usdkrw()
        with MARKET.lock:
            MARKET.upbit, MARKET.ts_upbit = up, now
            MARKET.binance, MARKET.ts_binance = bp, now
            MARKET.usdkrw, MARKET.ts_fx = fx, now

    # 김프 = 업비트 / (바낸 * 환율) - 1
    k = ((up - bp * fx) / (bp * fx)) * 100.0
    return round(k, 2), up, bp, fx


# ===================== Price & FX (주문·잔고 등 기타에서 사용) =====================
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
        v = float(text[s:e].replace(",", ""))
        return v if 900.0 <= v <= 2000.0 else 1300.0
    except Exception:
        return 1300.0


def calc_kimp_snapshot() -> Tuple[float, float, float, float]:
    """표시용 스냅샷: 캐시 우선"""
    return get_market_snapshot()


def calc_kimp_precise() -> Tuple[float, float, float, float]:
    """판정용: 캐시 우선(스테일시 자동 폴백)"""
    return get_market_snapshot()


# ===================== Balance =====================
def get_upbit_balance_real() -> Tuple[float, float]:
    keys = load_api_keys()
    access_key = keys.get("upbit_key", "")
    secret_key = keys.get("upbit_secret", "")

    payload = {"access_key": access_key, "nonce": str(uuid.uuid4())}
    jwt_token = jwt.encode(payload, secret_key, algorithm="HS256")
    if isinstance(jwt_token, (bytes, bytearray)):
        jwt_token = jwt_token.decode()
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
                # 표시용: locked 포함
                btc = float(b["balance"]) + float(b["locked"])
        return krw, btc
    except Exception as e:
        with state_lock:
            metrics["api_errors"] += 1
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
        with state_lock:
            metrics["api_errors"] += 1
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
        with state_lock:
            metrics["api_errors"] += 1
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
        with state_lock:
            metrics["api_errors"] += 1
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
        with state_lock:
            metrics["api_errors"] += 1
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
            with state_lock:
                metrics["api_errors"] += 1
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
            # 최소 금액 사전 차단
            if int(price_krw) < UPBIT_MIN_KRW_ORDER:
                log(f"[업비트 주문 차단] 금액 {int(price_krw)} KRW < 최소 {UPBIT_MIN_KRW_ORDER} KRW")
                return False
            query = {
                "market": "KRW-BTC",
                "side": "bid",
                "ord_type": "price",
                "price": str(int(price_krw)),
            }
        else:
            vol = floor_step(float(volume_btc), UPBIT_BTC_STEP)
            if vol < UPBIT_BTC_STEP - 1e-12:
                # 먼지는 실질 0
                log(f"[업비트 매도 스킵] 잔량 {volume_btc:.8f} BTC < 최소 {UPBIT_BTC_STEP}")
                return True
            query = {
                "market": "KRW-BTC",
                "side": "ask",
                "ord_type": "market",
                "volume": format(vol, ".8f"),
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
        if isinstance(jwt_token, (bytes, bytearray)):
            jwt_token = jwt_token.decode()
        headers = {"Authorization": f"Bearer {jwt_token}"}

        r = HTTP.post("https://api.upbit.com/v1/orders", params=query, headers=headers, timeout=TIMEOUT)
        with state_lock:
            metrics["orders_upbit"] += 1
        log(f"[업비트 주문] {side.upper()} vol={query.get('volume','')} price={query.get('price','')} → {_safe_json(r)}")
        return r.status_code == 201
    except Exception as e:
        with state_lock:
            metrics["api_errors"] += 1
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
        with state_lock:
            metrics["orders_binance"] += 1
        log(f"[바이낸스 주문] {side.upper()} qty={format(qty,'.3f')} reduceOnly={reduce_only} → {_safe_json(r)}")
        return r.status_code == 200
    except Exception as e:
        with state_lock:
            metrics["api_errors"] += 1
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
            log_json("size_adjust", action="sell_add", diff_usdt=round(diff,2), qty=qty)
        else:
            binance_order("buy", qty, reduce_only=True)
            log_json("size_adjust", action="buy_reduce", diff_usdt=round(-diff,2), qty=qty)
        time.sleep(0.35)


# ===================== Strategy helpers (무롤백 동시 진입 & 동시 청산) =====================
def can_enter_now(amount_krw: float, leverage: int, fx: float, bi_price: float, margin_buffer: float = 1.02) -> Tuple[bool, str, Dict[str, float]]:
    """
    진입 전에 둘 다 '거의 확실히' 가능한지 확인
    margin_buffer: 증거금 여유(예: +2%)
    """
    info: Dict[str, float] = {}
    # 업비트 KRW 체크
    krw, _ = get_upbit_balance_real()
    if krw < amount_krw:
        return False, f"업비트 KRW 부족 {int(krw):,} < {int(amount_krw):,}", info
    # 바이낸스 마진 체크
    target_size_usdt = amount_krw / fx
    need_margin = (target_size_usdt / leverage) * margin_buffer
    avail_usdt = get_binance_available_usdt()
    info.update(target_size_usdt=target_size_usdt, need_margin=need_margin, avail_usdt=avail_usdt)
    if avail_usdt + 1e-6 < need_margin:
        return False, f"바이낸스 증거금 부족 {avail_usdt:.2f} < {need_margin:.2f}", info
    return True, "OK", info


def enter_both_sides_once_no_rollback(
    amount_krw: float,
    fx: float,
    bi_price: float,
    leverage: int = 3,
    retries: int = 3,
    wait_sec: float = 0.4,
    confirm_timeout_sec: float = 3.0
) -> tuple[bool, float, float]:
    """
    - 업비트 금액시장가와 바이낸스 수량시장가를 동시에 발사
    - 실패한 쪽만 짧게 재시도(retries)
    - 롤백은 절대 하지 않음
    - 제한 시간 안에 양쪽 '실측' 보유/포지션이 확인되면 성공으로 간주
    반환: (성공여부, 확정_upbit_qty(추정), 최초_binance_qty)
    """
    ok_u = False
    ok_b = False

    target_size_usdt = amount_krw / fx
    init_qty = floor_step((target_size_usdt * 0.999) / bi_price, 0.001)  # -0.1% 여유
    if init_qty <= 0:
        log("[진입 중단] 계산된 초기 수량이 0")
        return False, 0.0, 0.0

    upbit_qty_est = round(amount_krw / get_upbit_price(), 3)  # 표시용 추정치

    # 1) 동시 발사
    def fire_upbit():
        nonlocal ok_u
        ok_u = upbit_order("buy", amount_krw, 0.0)

    def fire_binance():
        nonlocal ok_b
        ok_b = binance_order("sell", init_qty, reduce_only=False)

    t1 = threading.Thread(target=fire_upbit); t2 = threading.Thread(target=fire_binance)
    t1.start(); t2.start()
    t1.join(timeout=2.0); t2.join(timeout=2.0)

    # 2) 실패한 쪽만 짧게 재시도
    for _ in range(retries):
        if not ok_u:
            time.sleep(wait_sec)
            ok_u = upbit_order("buy", amount_krw, 0.0) or ok_u
        if not ok_b:
            time.sleep(wait_sec)
            ok_b = binance_order("sell", init_qty, reduce_only=False) or ok_b
        if ok_u and ok_b:
            break

    # 3) 바이낸스 명목가치 보정
    if ok_b:
        time.sleep(0.5)
        adjust_binance_size_to_target(target_size_usdt, ref_price=bi_price, tol_usdt=5.0)

    # 4) 제한 시간 내 실측 확인
    t_end = time.time() + confirm_timeout_sec
    entered = False
    while time.time() < t_end:
        _, ub_qty_real = get_upbit_balance_real()
        bn_qty = abs(get_binance_position_qty())
        if ub_qty_real >= UPBIT_BTC_STEP and bn_qty > 1e-6:
            entered = True
            break
        time.sleep(0.25)

    if entered:
        log("[진입 확정] 롤백 없이 양쪽 체결 확인")
        return True, upbit_qty_est, init_qty
    else:
        log("[경고] 부분 체결 상태 유지(롤백 비활성). 다음 루프에서 계속 보정")
        return False, upbit_qty_est if ok_u else 0.0, init_qty if ok_b else 0.0


def full_exit_with_retries(max_retries: int = 6, retry_delay: float = 1.2) -> bool:
    """
    업비트/바이낸스 동시 청산을 재시도하며 수행.
    - 업비트: BTC 잔량을 0.0001 스텝으로 내림, DUST 이하는 0으로 간주
    - 바이낸스: 포지션이 음수(숏)면 reduceOnly 시장가로 청산
    """
    for attempt in range(1, max_retries + 1):
        _, ub_raw = get_upbit_balance_real()
        ub = 0.0 if ub_raw <= DUST_BTC else floor_step(ub_raw, UPBIT_BTC_STEP)
        pos = get_binance_position_qty()  # 숏이면 음수
        need_up = ub >= UPBIT_BTC_STEP
        need_bn = pos < -1e-6

        log(f"[청산 체크#{attempt}] upbit={ub_raw:.8f}->{ub:.8f} BTC, binance_pos={pos}")

        ok_u = True
        ok_b = True
        if need_up:
            ok_u = upbit_order("sell", 0.0, ub)
        if need_bn:
            ok_b = binance_order("buy", abs(pos), reduce_only=True)

        time.sleep(retry_delay)

        _, ub_raw2 = get_upbit_balance_real()
        ub2 = 0.0 if ub_raw2 <= DUST_BTC else floor_step(ub_raw2, UPBIT_BTC_STEP)
        pos2 = get_binance_position_qty()
        closed = (ub2 < UPBIT_BTC_STEP) and (-1e-6 <= pos2 <= 1e-6)

        log(f"[청산 재확인#{attempt}] upbit={ub_raw2:.8f}->{ub2:.8f}, binance_pos={pos2}, result={'OK' if closed else 'RETRY'}")
        if ok_u and ok_b and closed:
            return True
    return False


# ===================== Strategy Core =====================
def _cooldown() -> bool:
    global _last_action_ts
    now = time.time()
    if now - _last_action_ts < COOLDOWN_SEC:
        return True
    _last_action_ts = now
    return False


def _validate_cfg(raw: Dict[str, Any]) -> Dict[str, Any]:
    """입력 파라미터 정규화/검증"""
    def f(name: str, default: float, lo: float, hi: float) -> float:
        try:
            v = float(raw.get(name, default))
        except Exception:
            v = default
        return max(lo, min(hi, v))

    cfg = {
        "target_kimp": f("target_kimp", 0.0, -10.0, 10.0),
        "exit_kimp":   f("exit_kimp",   0.3, -10.0, 10.0),
        "tolerance":   f("tolerance",   0.1,  0.0,  5.0),
        "amount_krw":  f("amount_krw",  1_000_000, 10_000, 100_000_000),
        "leverage":    int(f("leverage", 3, 1, 20)),
    }
    return cfg


def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global position_state, trade_count, entry_info, entry_kimp_value, exiting

    target_kimp = float(cfg["target_kimp"])
    exit_kimp   = float(cfg["exit_kimp"])
    tolerance   = float(cfg["tolerance"])
    amount_krw  = float(cfg["amount_krw"])
    leverage    = int(float(cfg["leverage"]))

    ensure_binance_margin_and_leverage("BTCUSDT", leverage)

    while running_evt.is_set():
        try:
            with state_lock:
                metrics["loops"] += 1

            # 쿨다운(너무 잦은 액션 방지)
            if _cooldown():
                time.sleep(0.25)
                continue

            # ▶ 판정용 김프: 캐시 스냅샷(스테일이면 자동 폴백)
            kimp, up, bp, fx = calc_kimp_precise()
            kimp_view = round(kimp, 2)

            # ===== Entry =====
            if position_state == "neutral":
                dir_ok  = (kimp <= target_kimp) if target_kimp < 0 else (kimp >= target_kimp)
                near_ok = (kimp <= target_kimp + tolerance) if target_kimp < 0 else (kimp >= target_kimp - tolerance)

                log_json("check", kimp=kimp_view, target=target_kimp, tol=tolerance,
                         dir_ok=dir_ok, near_ok=near_ok, state=position_state)

                if not (dir_ok and near_ok):
                    time.sleep(0.4)
                    continue

                # 프리체크(잔고/증거금 여유)
                ok_gate, msg, info = can_enter_now(amount_krw, leverage, fx, bp, margin_buffer=1.02)
                if not ok_gate:
                    log(f"[진입 중단] {msg}")
                    time.sleep(0.6)
                    continue

                log(f"[프리체크 OK] 목표≈{info['target_size_usdt']:.2f}USDT 필요증거금≈{info['need_margin']:.2f} 가용≈{info['avail_usdt']:.2f}")

                # 무(無)롤백 동시 진입 + 실패쪽만 재시도
                ok_enter, upbit_qty_est, init_qty = enter_both_sides_once_no_rollback(
                    amount_krw, fx, bp, leverage=leverage, retries=3, wait_sec=0.4, confirm_timeout_sec=3.0
                )

                if not ok_enter:
                    # 다음 루프에서 계속 보정/감시
                    time.sleep(0.6)
                    # 혹시 이미 완성됐는지 한 번 더 확인
                    _, ub_qty_real = get_upbit_balance_real()
                    bn_qty = abs(get_binance_position_qty())
                    if ub_qty_real >= UPBIT_BTC_STEP and bn_qty > 1e-6:
                        entry_info["upbit_qty"] = floor_step(ub_qty_real, UPBIT_BTC_STEP)
                        entry_info["binance_qty"] = floor_step(bn_qty, 0.001)
                        position_state = "entered"
                        entry_kimp_value = float(kimp)
                        log("[진입 확정] 부분 체결 → 다음 루프에서 완성 확인")
                    continue

                # 진입 성공 시 명목가치 정밀 보정(한 번 더)
                time.sleep(0.5)
                target_size_usdt = amount_krw / fx
                adjust_binance_size_to_target(target_size_usdt, ref_price=bp, tol_usdt=5.0)

                # 최종 실측으로 상태 확정
                _, ub_qty_real = get_upbit_balance_real()
                bn_qty = abs(get_binance_position_qty())
                if ub_qty_real >= UPBIT_BTC_STEP and bn_qty > 1e-6:
                    entry_info["upbit_qty"] = floor_step(ub_qty_real, UPBIT_BTC_STEP)
                    entry_info["binance_qty"] = floor_step(bn_qty, 0.001)
                    position_state = "entered"
                    entry_kimp_value = float(kimp)
                    log(f"[진입 확정] 업비트 +{entry_info['upbit_qty']}BTC(≈{int(amount_krw)}KRW) / "
                        f"바이낸스 숏 ≈{round(target_size_usdt,2)}USDT (x{leverage}) @ 김프 {kimp_view}%")
                    log_json("enter", kimp=kimp_view, entry_qty_upbit=entry_info['upbit_qty'],
                             target_size_usdt=round(target_size_usdt,2), lev=leverage)
                else:
                    log(f"[진입 미완성] ub={ub_qty_real:.6f} BTC, bn={bn_qty:.6f} BTC — 다음 루프에서 보정")
                continue

            # ===== Exit =====
            if position_state == "entered":
                if exiting:
                    time.sleep(0.2)
                    continue

                # 청산 직전 재계산
                kimp2, up2, bp2, fx2 = calc_kimp_precise()
                kimp2_view = round(kimp2, 2)

                crossed = False
                reason = ""
                if entry_kimp_value is not None:
                    if exit_kimp >= entry_kimp_value:
                        crossed = (kimp2 >= exit_kimp)
                        reason = f"kimp_now({kimp2_view}) >= exit_kimp({exit_kimp})"
                    else:
                        crossed = (kimp2 <= exit_kimp)
                        reason = f"kimp_now({kimp2_view}) <= exit_kimp({exit_kimp})"

                if crossed:
                    exiting = True
                    log_json("exit_trigger", entry=entry_kimp_value, exit_target=exit_kimp, reason=reason)

                    done = full_exit_with_retries(max_retries=6, retry_delay=1.2)
                    if done:
                        with state_lock:
                            global trade_count
                            trade_count += 1
                        position_state = "neutral"
                        entry_info.update({"upbit_qty": 0.0, "binance_qty": 0.0})
                        log(f"[청산 성공] 김프 {kimp2_view}% (진입 {entry_kimp_value} → 청산선 {exit_kimp})")
                        log_json("exit_done", kimp=kimp2_view)
                        entry_kimp_value = None
                        time.sleep(0.6)
                    else:
                        log("[청산 미완료] 잔량 남음. 루프 재시도")
                    exiting = False
                continue

        except Exception as e:
            with state_lock:
                metrics["api_errors"] += 1
            log(f"[전략 오류] {e}")

        time.sleep(0.35)


# ===================== Routes =====================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "ts": int(time.time())})


@app.route("/health")
def health():
    with state_lock:
        return jsonify({
            "running": running_evt.is_set(),
            "position_state": position_state,
            "thread_alive": (strategy_thread.is_alive() if strategy_thread else False),
            "loops": metrics["loops"],
            "api_errors": metrics["api_errors"],
        })


@app.route("/metrics")
def _metrics():
    with state_lock:
        return jsonify(metrics)


@app.route("/config", methods=["GET"])
def get_config():
    return jsonify({
        "cooldown_sec": COOLDOWN_SEC,
        "max_logs": MAX_LOGS
    })


@app.route("/current_kimp")
def current_kimp():
    # 캐시 스냅샷(스테일시 1회 폴백)
    k, up, bp, fx = calc_kimp_snapshot()
    return jsonify({"kimp": k, "upbit_price": up, "binance_price": bp, "usdkrw": fx})


@app.route("/balance")
def balance():
    krw, btc = get_upbit_balance_real()
    usdt = get_binance_available_usdt()
    return jsonify({"real": {"krw": round(krw, 0), "btc_upbit": round(btc, 6), "usdt": round(usdt, 3)}})


@app.route("/status")
def status():
    with state_lock:
        return jsonify(
            {
                "running": running_evt.is_set(),
                "position_state": position_state,
                "trade_count": trade_count,
                "profit_krw": round(profit_krw),
                "total_pnl": total_pnl,
                "logs": list(logs)[-400:],           # 과다 응답 방지
                "logs_json": list(logs_json)[-400:], # 구조화 로그
                "entry_info": entry_info,
            }
        )


@app.route("/start", methods=["POST"])
def start():
    global strategy_thread
    cfg_raw = request.json or {}
    cfg = _validate_cfg(cfg_raw)

    # 초고속 캐시 스레드 보장
    ensure_market_updater()

    # 중복 시작 방지
    if running_evt.is_set() and strategy_thread and strategy_thread.is_alive():
        log_json("start_skip", reason="already_running", cfg=cfg)
        return jsonify({"status": "already_running", "cfg": cfg}), 200

    # 상태 리셋
    with state_lock:
        logs.clear(); logs_json.clear()
        metrics["loops"] = 0
        metrics["api_errors"] = 0

    running_evt.set()
    strategy_thread = threading.Thread(target=run_strategy_thread, args=(cfg,), daemon=True)
    strategy_thread.start()
    log_json("start", cfg=cfg)
    return jsonify({"status": "started", "cfg": cfg}), 200


@app.route("/stop", methods=["POST"])
def stop():
    running_evt.clear()
    log_json("stop_called")
    time.sleep(0.2)  # 스레드 자연 종료 대기
    return jsonify({"status": "stopped"}), 200


if __name__ == "__main__":
    # 서버 부팅 시 캐시 스레드 선구동(선택)
    ensure_market_updater()
    # 프로덕션: debug=False, 프록시 뒤에서 운영 권장
    app.run(host="0.0.0.0", port=5000, debug=False)
