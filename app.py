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

    entry_upbit_qty = 0.0

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
                if target_kimp < 0:
                    near_ok = (kimp <= target_kimp + tolerance)
                else:
                    near_ok = (kimp >= target_kimp - tolerance)

                log_json("check", kimp=kimp_view, target=target_kimp, tol=tolerance,
                         near_ok=near_ok, state=position_state)

                if not near_ok:
                    time.sleep(0.4)
                    continue

                upbit_qty = round(amount_krw / up, 3)

                target_size_usdt = amount_krw / fx
                need_margin_usdt = target_size_usdt / leverage
                avail_usdt = get_binance_available_usdt()
                log(f"[검사] 목표사이즈≈{round(target_size_usdt,2)}USDT, 필요증거금≈{round(need_margin_usdt,2)}USDT, 가용≈{round(avail_usdt,2)}USDT")

                if avail_usdt + 1e-6 < need_margin_usdt:
                    log("[진입 중단] 바이낸스 가용 USDT가 필요 증거금보다 적습니다.")
                    time.sleep(1.0)
                    continue

                ok_upbit = upbit_order("buy", amount_krw, upbit_qty)
                if not ok_upbit:
                    log("[진입 실패] 업비트 매수 실패")
                    time.sleep(0.6)
                    continue

                first_qty = floor_step(target_size_usdt / bp, 0.001)
                ok_binance = binance_order("sell", first_qty, reduce_only=False)
                if not ok_binance:
                    log("[진입 실패] 바이낸스 숏 실패 → 업비트 되돌림")
                    upbit_order("sell", 0.0, upbit_qty)
                    time.sleep(0.6)
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
                    f"@ 김프 {kimp_view}%")
                log_json("enter", kimp=kimp_view, entry_qty_upbit=upbit_qty,
                         target_size_usdt=round(target_size_usdt,2), lev=leverage)
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

                    ok_upbit = upbit_order("sell", 0.0, entry_upbit_qty)
                    pos_amt = get_binance_position_qty()
                    ok_binance = True
                    if pos_amt < -1e-6:
                        ok_binance = binance_order("buy", abs(pos_amt), reduce_only=True)

                    if ok_upbit and ok_binance:
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
                        if not ok_upbit and ok_binance:
                            log("[청산 일부실패] 업비트 실패 → 바이낸스 되돌림 필요 가능")
                            log_json("exit_partial", side="upbit_fail")
                        elif ok_upbit and not ok_binance:
                            log("[청산 일부실패] 바이낸스 실패 → 업비트 되돌림 필요 가능")
                            log_json("exit_partial", side="binance_fail")
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
