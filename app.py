# app_multi_band_final.py — Upbit Spot LONG + Binance Futures SHORT
# Multi-Band / Queue + Auto Re-Arm, matched-qty PnL, residual trim, reconciliation worker
from __future__ import annotations

import os, time, uuid, hmac, jwt, hashlib, threading, math
from typing import Any, Dict, Tuple, Optional, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import urlencode
from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from api.api_key import load_api_keys

# ---------------- Flask ----------------
app = Flask(__name__)
CORS(app)

# ---------------- Robust HTTP ----------------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(
        total=5, backoff_factor=0.5,
        status_forcelist=(408,429,500,502,503,504),
        allowed_methods=frozenset(["GET","POST"]),
        raise_on_status=False
    )
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

HTTP = _make_session()
TIMEOUT = (3, 7)

# ---------------- Fees/rounding ----------------
UPBIT_TAKER_FEE   = float(os.getenv("UPBIT_TAKER_FEE",  "0.0005"))  # 0.05%
BINANCE_TAKER_FEE = float(os.getenv("BINANCE_TAKER_FEE","0.0004"))  # 0.04%

# 목표 순이익(+0.10%p) 기본과 슬리피지 버퍼(기본 0.05%p)
TARGET_NET_PROFIT_BP = float(os.getenv("TARGET_NET_PROFIT_BP","0.10"))  # %p
SLIPPAGE_BP_BUFFER   = float(os.getenv("SLIPPAGE_BP_BUFFER","0.05"))    # %p (하한)
FEES_BP = (UPBIT_TAKER_FEE*2 + BINANCE_TAKER_FEE*2) * 100.0             # ≈0.18%p

# ✅ 청산 최소 순이익 가드 + 보유 상한
MIN_NET_EXIT_BP = float(os.getenv("MIN_NET_EXIT_BP","0.08"))  # %p, 추정 순이익이 이 값 미만이면 청산 보류
MAX_HOLD_SEC    = int(float(os.getenv("MAX_HOLD_SEC","120"))) # 초, 초과시 가드 무시하고 청산 시도

# ✅ 즉시청산 방지(진입 직후 n초 동안 자동 청산 금지)
MIN_HOLD_BEFORE_EXIT_SEC = int(float(os.getenv("MIN_HOLD_BEFORE_EXIT_SEC","3")))

# ✅ 잔량 트림/리컨실 설정
TRIM_EPS = float(os.getenv("TRIM_EPS","0.0005"))  # |upbit - binance| >= EPS → 트림
RECONCILE_INTERVAL_SEC = float(os.getenv("RECONCILE_INTERVAL_SEC","2"))

# ✅ 슬리피지 적응형(EMA)
_slip_ema = SLIPPAGE_BP_BUFFER
_SLIP_ALPHA = 0.2

def _update_slip_ema_from_observed(slip_bp: float):
    global _slip_ema
    slip_bp = max(0.0, float(slip_bp))
    _slip_ema = (1.0 - _SLIP_ALPHA) * _slip_ema + _SLIP_ALPHA * slip_bp

def _current_slip_buffer()->float:
    return max(SLIPPAGE_BP_BUFFER, _slip_ema)

def required_spread_bp()->float:
    return round(FEES_BP + TARGET_NET_PROFIT_BP + _current_slip_buffer(), 4)

def floor_step(x: float, step: float = 0.001) -> float:
    return math.floor(float(x)/step)*step

# ---------------- Entry tuning ----------------
BN_LOT_STEP = float(os.getenv("BN_LOT_STEP", "0.006"))
UNDERBUY_RATIO = float(os.getenv("UNDERBUY_RATIO", "0.998"))
FAST_EXIT_NO_GUARD = str(os.getenv("FAST_EXIT_NO_GUARD","true")).lower() in ("1","true","yes")
UPBIT_VOL_PRECISION = int(os.getenv("UPBIT_VOL_PRECISION", "8"))

def fmt_upbit_qty(q: float) -> str:
    return f"{float(q):.{UPBIT_VOL_PRECISION}f}"

# ---------------- Auto sizing ----------------
AUTO_SIZE_DEFAULT = True
KRW_BUF_RATIO     = float(os.getenv("KRW_BUF_RATIO","0.001"))
USDT_FEE_BUFFER   = float(os.getenv("USDT_FEE_BUFFER","1.0"))

# ---------------- Exit policy ----------------
# by_band_safe  : 밴드 구조량을 목표로 하되, 실제 청산 수량은 '바이낸스 열린 포지션'과 '업비트 보유량'을 넘지 않도록 캡(권장)
# by_band_strict: 밴드 구조량만큼 업비트를 우선 청산(바이낸스가 부족하면 일부 비헤지 구간이 생길 수 있음 — 고위험)
EXIT_MODE = os.getenv("EXIT_MODE", "by_band_safe")  # "by_band_safe" | "by_band_strict"

# ---------------- Global State ----------------
running = False
metrics = {"loops":0, "orders_binance":0, "orders_upbit":0, "api_errors":0}
logs: List[str] = []
last_error: Optional[str] = None

# 누적 수치
profit_krw = 0.0
fees_upbit_krw_cum = 0.0
fees_binance_usdt_cum = 0.0
fees_binance_krw_cum = 0.0

# ▶ 최근 청산 / 미실현 분리
last_realized_pnl_krw = 0.0
recent_closes: List[Dict[str, Any]] = []
MAX_RECENT_CLOSES = 20

# 밴드 큐
# state: waiting → hedging → entered → closed / cancelled
bands: Dict[str, Dict[str, Any]] = {}

# 기본 옵션
DEFAULT_TOLERANCE = float(os.getenv("DEFAULT_TOLERANCE","0.10"))
DEFAULT_LEVERAGE  = int(float(os.getenv("DEFAULT_LEVERAGE","3")))
DEFAULT_EXIT_ON_SIGN_CHANGE = False
DEFAULT_EXIT_ON_MOVE_BP = 0.0

# 자동 재무장 기본값
REPEAT_DEFAULT = str(os.getenv("REPEAT_DEFAULT","true")).lower() in ("1","true","yes")
REPEAT_COOLDOWN_DEFAULT = int(float(os.getenv("REPEAT_COOLDOWN_SEC","0")))

# ---------------- Binance base ----------------
BINANCE_TESTNET = str(os.getenv("BINANCE_TESTNET","false")).lower() in ("1","true","yes")
BASE_FAPI = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"
RECV_WINDOW = 5000
_bn_time_offset_ms = 0


def _bn_headers()->dict:
    k = load_api_keys()
    return {"X-MBX-APIKEY": k.get("binance_key",""),
            "Content-Type":"application/x-www-form-urlencoded"}


def _bn_now_ms()->int: return int(time.time()*1000)+_bn_time_offset_ms


def _bn_sign(params: Dict[str,Any])->str:
    k=load_api_keys(); sec=k.get("binance_secret","")
    qs=urlencode(params, doseq=True)
    sig=hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs+"&signature="+sig


def _is_ts_error(t: str) -> bool:
    t=(t or "").lower()
    return "-1021" in t or ("timestamp" in t and ("ahead" in t or "server time" in t or "recvwindow" in t))


def sync_binance_time():
    global _bn_time_offset_ms
    try:
        r = HTTP.get(f"{BASE_FAPI}/fapi/v1/time", timeout=TIMEOUT)
        r.raise_for_status()
        st = int(r.json()["serverTime"])
        _bn_time_offset_ms = st - int(time.time()*1000)
        log(f"[바이낸스 시간동기] offset={_bn_time_offset_ms}ms (testnet={BINANCE_TESTNET})")
    except Exception as e:
        _bn_time_offset_ms = 0; log(f"[시간동기 실패] {e}")


def _bn_signed_get(path, params=None, retry=1):
    params = params or {}; params["timestamp"]=_bn_now_ms(); params["recvWindow"]=RECV_WINDOW
    url = f"{BASE_FAPI}{path}?{_bn_sign(params)}"
    r = HTTP.get(url, headers=_bn_headers(), timeout=TIMEOUT)
    if r.status_code==400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021(GET) → 재동기"); sync_binance_time(); return _bn_signed_get(path, params, retry-1)
    return r


def _bn_signed_post(path, params=None, retry=1):
    params = params or {}; params["timestamp"]=_bn_now_ms(); params["recvWindow"]=RECV_WINDOW
    r = HTTP.post(f"{BASE_FAPI}{path}", headers=_bn_headers(), data=_bn_sign(params), timeout=TIMEOUT)
    if r.status_code==400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021(POST) → 재동기"); sync_binance_time(); return _bn_signed_post(path, params, retry-1)
    return r

# ---------------- Prices & FX ----------------
_FX_CACHE_TS=0.0; _FX_CACHE_VAL=1300.0; _FX_TTL=30.0

def get_upbit_price()->float:
    r=HTTP.get("https://api.upbit.com/v1/ticker?markets=KRW-BTC", timeout=TIMEOUT)
    r.raise_for_status(); return float(r.json()[0]["trade_price"])


def get_binance_price()->float:
    r=HTTP.get(f"{BASE_FAPI}/fapi/v1/ticker/price", params={"symbol":"BTCUSDT"}, timeout=TIMEOUT)
    r.raise_for_status(); return float(r.json()["price"])


def get_usdkrw()->float:
    global _FX_CACHE_TS,_FX_CACHE_VAL
    now=time.time()
    if now-_FX_CACHE_TS<_FX_TTL: return _FX_CACHE_VAL
    try:
        html=HTTP.get("https://www.google.com/finance/quote/USD-KRW",
                      headers={"User-Agent":"Mozilla/5.0"}, timeout=TIMEOUT).text
        key='data-last-price="'; i=html.find(key)
        if i!=-1:
            j=html.find('"', i+len(key))
            fx=float(html[i+len(key):j].replace(",",""))
            _FX_CACHE_TS,_FX_CACHE_VAL=now,fx; return fx
    except Exception as e: log(f"[FX] 구글 실패:{e} → KRW-USDT")
    try:
        r=HTTP.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=TIMEOUT); r.raise_for_status()
        fx=float(r.json()[0]["trade_price"]); _FX_CACHE_TS,_FX_CACHE_VAL=now,fx; return fx
    except Exception as e: log(f"[FX] 폴백 실패:{e}")
    _FX_CACHE_TS,_FX_CACHE_VAL=now,1300.0; return 1300.0


def calc_kimp()->Tuple[float,float,float,float]:
    up,bp,fx = get_upbit_price(), get_binance_price(), get_usdkrw()
    k=((up-bp*fx)/(bp*fx))*100.0
    return round(k,2), up, bp, fx

# ---------------- Balances ----------------

def get_upbit_balance_real()->Tuple[float,float]:
    k=load_api_keys(); ak=k.get("upbit_key",""); sk=k.get("upbit_secret","")
    payload={"access_key":ak,"nonce":str(uuid.uuid4())}
    jwt_token=jwt.encode(payload, sk, algorithm="HS256")
    try:
        r=HTTP.get("https://api.upbit.com/v1/accounts",
                   headers={"Authorization":f"Bearer {jwt_token}"}, timeout=TIMEOUT)
        r.raise_for_status(); krw=btc=0.0
        for b in r.json():
            if b["currency"]=="KRW": krw=float(b["balance"])
            elif b["currency"]=="BTC": btc=float(b["balance"]) + float(b["locked"])
        return krw, btc
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[UPBIT 잔고 오류]{e}"); return 0.0,0.0


def get_binance_balance_real()->float:
    try:
        r=_bn_signed_get("/fapi/v2/balance")
        if r.status_code!=200:
            metrics["api_errors"]+=1; log(f"[BINANCE 잔고 오류]{r.status_code}:{r.text}")
            if '"code":-2015' in r.text: globals()["last_error"]="binance_invalid_key_or_permission (-2015)"
            return 0.0
        for b in r.json():
            if b.get("asset")=="USDT": return round(float(b.get("balance",0.0)),3)
        return 0.0
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[BINANCE 잔고 예외]{e}"); return 0.0

# ---------------- Orders & Fills ----------------

def set_binance_leverage(symbol:str, leverage:int)->bool:
    try:
        r=_bn_signed_post("/fapi/v1/leverage",{"symbol":symbol,"leverage":leverage})
        log(f"[레버리지 설정] {symbol} x{leverage} → status={r.status_code}")
        if r.status_code!=200 and '"code":-2015' in r.text: globals()["last_error"]="binance_invalid_key_or_permission (-2015)"
        return r.status_code==200
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[레버리지 오류]{e}"); return False


def upbit_order(side:str, price:float, volume:float)->Tuple[bool,Optional[str]]:
    k=load_api_keys(); ak=k.get("upbit_key",""); sk=k.get("upbit_secret","")
    if side=="buy":
        q={"market":"KRW-BTC","side":"bid","ord_type":"price","price":str(int(price))}
    else:
        q={"market":"KRW-BTC","side":"ask","ord_type":"market","volume":format(float(volume),".3f")}
    m=hashlib.sha512(); m.update(urlencode(q).encode())
    payload={"access_key":ak,"nonce":str(uuid.uuid4()),"query_hash":m.hexdigest(),"query_hash_alg":"SHA512"}
    jwt_token=jwt.encode(payload, sk, algorithm="HS256")
    r=HTTP.post("https://api.upbit.com/v1/orders", params=q,
                headers={"Authorization":f"Bearer {jwt_token}"}, timeout=TIMEOUT)
    try: data=r.json()
    except Exception: data={"raw":r.text}
    log(f"[업비트 주문] {side.upper()} → {data}")
    if r.status_code==201 and isinstance(data,dict) and "uuid" in data:
        metrics["orders_upbit"]+=1; return True, data["uuid"]
    return False, None


def upbit_order_detail(uuid_str:str)->dict:
    k=load_api_keys(); ak=k.get("upbit_key",""); sk=k.get("upbit_secret","")
    q={"uuid":uuid_str}; m=hashlib.sha512(); m.update(urlencode(q).encode())
    payload={"access_key":ak,"nonce":str(uuid.uuid4()),"query_hash":m.hexdigest(),"query_hash_alg":"SHA512"}
    jwt_token=jwt.encode(payload, sk, algorithm="HS256")
    r=HTTP.get("https://api.upbit.com/v1/order",
               headers={"Authorization":f"Bearer {jwt_token}"}, params=q, timeout=TIMEOUT)
    return r.json() if r.status_code==200 else {"error":r.text}


def binance_order(side:str, price:float, volume:float, reduce_only:bool=False)->Tuple[bool,Optional[int]]:
    try:
        params={"symbol":"BTCUSDT","side":"SELL" if side=="sell" else "BUY",
                "type":"MARKET","quantity":format(float(volume),".3f")}
        if reduce_only: params["reduceOnly"]="true"
        r=_bn_signed_post("/fapi/v1/order", params)
        try: data=r.json()
        except Exception: data={"raw":r.text}
        log(f"[바이낸스 주문] {side.UPPER()} qty={params['quantity']} RO={reduce_only} → {r.status_code} {data}")
        if r.status_code==200 and isinstance(data,dict) and "orderId" in data:
            metrics["orders_binance"]+=1; return True, int(data["orderId"])
        if r.status_code!=200 and '"code":-2015' in r.text: globals()["last_error"]="binance_invalid_key_or_permission (-2015)"
        return False, None
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[바이낸스 주문 오류]{e}"); return False, None


def binance_user_trades(order_id:int)->List[dict]:
    r=_bn_signed_get("/fapi/v1/userTrades", {"symbol":"BTCUSDT","orderId":order_id,"timestamp":_bn_now_ms()})
    return r.json() if r.status_code==200 else []


def summarize_upbit_order(uuid_str:str)->Tuple[float,float,float]:
    d=upbit_order_detail(uuid_str)
    fee=float(d.get("paid_fee","0") or 0.0); funds=vol=0.0
    for t in d.get("trades",[]):
        vol+=float(t.get("volume","0") or 0.0); funds+=float(t.get("funds","0") or 0.0)
    return funds, vol, fee


def summarize_binance_order(order_id:int)->Tuple[float,float,float]:
    fills=binance_user_trades(order_id); qty=quote=fee=0.0
    for f in fills:
        q=float(f.get("qty","0") or 0.0); p=float(f.get("price","0") or 0.0); c=float(f.get("commission","0") or 0.0)
        qty+=q; quote+=q*p; fee+=c
    avg=(quote/qty) if qty>0 else 0.0
    return qty, avg, fee

# ---------------- Utils ----------------

def log(msg: str):
    ts = time.strftime("[%H:%M:%S]"); logs.append(f"{ts} {msg}")
    if len(logs) > 600: del logs[:250]


def _new_band_id()->str:
    return time.strftime("%H%M%S")+"-"+uuid.uuid4().hex[:4]


def _reset_band_trade_fields(band: Dict[str, Any]) -> None:
    band["pair_id"] = None
    band["matched_qty"] = 0.0
    band["entry_kimp_value"] = None
    band["entry_ts"] = None
    band["upbit"] = {"buy_uuid": None, "sell_uuid": None,
                     "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0}
    band["binance"] = {"sell_id": None, "buy_id": None,
                       "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                       "fee_usdt": 0.0, "entry_margin_usdt": 0.0}
    band["peak_kimp_after_entry"] = None


def _rearm_after_cooldown(band_id: str, cooldown: int) -> None:
    try:
        if cooldown > 0: time.sleep(cooldown)
        b = bands.get(band_id)
        if not b or not b.get("repeat", False) or b["state"] != "closed": return
        _reset_band_trade_fields(b)
        b["state"] = "waiting"
        b["times_armed"] = int(b.get("times_armed", 0)) + 1
        b["last_arm_ts"] = time.time()
        log(f"[자동 재무장] id={band_id} cooldown={cooldown}s → waiting")
    except Exception as e:
        log(f"[자동 재무장 오류] id={band_id} {e}")

# ---------------- PnL helpers ----------------

def compute_band_pnl_krw(band: Dict[str, Any], fx_for_exit: float) -> float:
    """
    ✅ 청산 확정 시 호출.
    - Upbit: (sell_krw - buy_krw - upbit_fee_krw)
    - Binance(Short): (entry_avg - exit_avg) * qty_close - fee_usdt
    - 합산 후 KRW
    - 수수료는 '증분'만 누적
    - 상세 로그 남김
    """
    global fees_upbit_krw_cum, fees_binance_usdt_cum, fees_binance_krw_cum

    up = band["upbit"]; bn = band["binance"]

    # ---- Upbit fills/fees ----
    buy_funds = buy_vol = buy_fee = 0.0
    if up.get("buy_uuid"):
        buy_funds, buy_vol, buy_fee = summarize_upbit_order(up["buy_uuid"])

    sell_funds = sell_fee = 0.0
    if up.get("sell_uuid"):
        sell_funds, _, sell_fee = summarize_upbit_order(up["sell_uuid"])

    up_fee_now  = float((buy_fee or 0.0) + (sell_fee or 0.0))
    up_fee_prev = float(up.get("fee_krw", 0.0))
    up_fee_delta = max(0.0, up_fee_now - up_fee_prev)  # 🔒 증분만 누적
    up["fee_krw"] = up_fee_now
    up["buy_krw"] = float(buy_funds or 0.0)
    up["sell_krw"] = float(sell_funds or 0.0)
    up["filled_qty"] = max(up.get("filled_qty", 0.0), round(float(buy_vol or 0.0), 3))
    fees_upbit_krw_cum += up_fee_delta

    upbit_pnl = up["sell_krw"] - up["buy_krw"] - up["fee_krw"]

    # ---- Binance fills/fees ----
    entry_qty = entry_avg = entry_fee = 0.0
    if bn.get("sell_id"):  # short entry
        entry_qty, entry_avg, entry_fee = summarize_binance_order(bn["sell_id"])

    exit_qty = exit_avg = exit_fee = 0.0
    if bn.get("buy_id"):   # reduce-only buy
        exit_qty, exit_avg, exit_fee = summarize_binance_order(bn["buy_id"])

    bn_fee_now  = float((entry_fee or 0.0) + (exit_fee or 0.0))
    bn_fee_prev = float(bn.get("fee_usdt", 0.0))
    bn_fee_delta = max(0.0, bn_fee_now - bn_fee_prev)
    bn["fee_usdt"] = bn_fee_now

    bn["entry_qty"], bn["entry_avg"] = float(entry_qty or 0.0), float(entry_avg or 0.0)
    bn["exit_qty"],  bn["exit_avg"]  = float(exit_qty or 0.0),  float(exit_avg or 0.0)

    qty_close = floor_step(min(bn["entry_qty"], bn["exit_qty"]), 0.001)

    fut_pnl_usdt_gross = (bn["entry_avg"] - bn["exit_avg"]) * qty_close
    fut_pnl_usdt = fut_pnl_usdt_gross - bn_fee_now

    total_krw = upbit_pnl + fut_pnl_usdt * fx_for_exit
    band["pnl_krw"] = total_krw
    band["last_pnl_krw"] = total_krw

    # 누적 수수료 집계 (USDT→KRW 환산 누적 포함)
    fees_binance_usdt_cum += bn_fee_delta
    fees_binance_krw_cum  += bn_fee_delta * fx_for_exit

    # 디버그/참고치: ΔK 이익 추정
    entry_k = band.get("entry_kimp_value")
    exit_k  = band.get("exit_kimp")
    delta_k_pp = None; notional_krw = None; delta_k_gain_est = None
    if entry_k is not None and exit_k is not None and qty_close > 0 and bn["exit_avg"] > 0:
        notional_krw = bn["exit_avg"] * fx_for_exit * qty_close
        delta_k_pp = float(exit_k) - float(entry_k)
        delta_k_gain_est = (delta_k_pp / 100.0) * notional_krw

    band["last_close_detail"] = {
        "qty_close": qty_close,
        "upbit_pnl": upbit_pnl,
        "fut_pnl_usdt": fut_pnl_usdt,
        "fx": fx_for_exit,
        "delta_k_pp": delta_k_pp,
        "notional_krw": notional_krw,
        "delta_k_gain_est": delta_k_gain_est,
        "fees": {
            "upbit_krw_now": up_fee_now,
            "binance_usdt_now": bn_fee_now,
        }
    }

    if delta_k_gain_est is not None and notional_krw is not None and delta_k_pp is not None:
        log(
            f"[실현손익/{band['id']}] {total_krw:+,.0f} KRW "
            f"(ΔK≈{delta_k_pp:+.2f}%p → {delta_k_gain_est:,.0f} | "
            f"업비트수수료={up_fee_now:,.0f} | "
            f"바낸수수료≈{bn_fee_now*fx_for_exit:,.0f}KRW | "
            f"Upbit {upbit_pnl:+,.0f} + Bin {fut_pnl_usdt:+.3f}USDT)"
        )
    else:
        log(f"[실현손익/{band['id']}] {total_krw:+,.0f} KRW (Upbit {upbit_pnl:+,.0f} + Bin {fut_pnl_usdt:+.3f}USDT)")
    return total_krw


def estimate_unrealized_pnl_krw(band: Dict[str, Any], up_now: float, bp_now: float, fx_now: float) -> float:
    up = band["upbit"]; bn = band["binance"]
    qty = float(band.get("matched_qty", up.get("filled_qty", 0.0)))
    if qty <= 0: return 0.0
    buy_krw = float(up.get("buy_krw", 0.0))
    buy_fee_krw = float(up.get("fee_krw", 0.0))
    avg_up = (buy_krw / max(1e-12, qty)) if qty>0 else up_now
    upbit_unreal = (up_now - avg_up) * qty - buy_fee_krw  # 단순 근사
    entry_avg = float(bn.get("entry_avg", 0.0))
    entry_qty = float(bn.get("entry_qty", 0.0))
    entry_fee_usdt = float(bn.get("fee_usdt", 0.0))
    fut_unreal_usdt = (entry_avg - bp_now) * min(qty, entry_qty) - entry_fee_usdt
    return upbit_unreal + fut_unreal_usdt * fx_now

# ---------------- Matched-qty & Trim/Reconcile ----------------

def _calc_matched_qty(band: Dict[str, Any]) -> float:
    up = band["upbit"]; bn = band["binance"]
    # 최신 체결 조회
    if up.get("buy_uuid"):
        _, up_vol, _ = summarize_upbit_order(up["buy_uuid"])
    else:
        up_vol = 0.0
    if bn.get("sell_id"):
        bn_qty, _, _ = summarize_binance_order(bn["sell_id"])
    else:
        bn_qty = 0.0
    matched = floor_step(min(up_vol, bn_qty), 0.001)
    band["matched_qty"] = matched
    band["upbit"]["filled_qty"] = floor_step(up_vol, 0.001)
    band["binance"]["entry_qty"] = floor_step(bn_qty, 0.001)
    return matched


def _trim_residual(band: Dict[str, Any]) -> None:
    """잔량 많은 쪽을 시장가 트림(ReduceOnly)으로 맞춤."""
    up = band["upbit"]; bn = band["binance"]
    # 현재 열린 수량 추정
    # upbit: 매수 체결 - (있다면) 매도 체결 수량
    up_buy_funds, up_buy_vol, _ = summarize_upbit_order(up.get("buy_uuid")) if up.get("buy_uuid") else (0.0,0.0,0.0)
    up_sell_funds, up_sell_vol, _ = summarize_upbit_order(up.get("sell_uuid")) if up.get("sell_uuid") else (0.0,0.0,0.0)
    up_open = round(up_buy_vol - up_sell_vol, 8)

    # binance: entry - exit
    bn_entry_qty, _, _ = summarize_binance_order(bn.get("sell_id")) if bn.get("sell_id") else (0.0,0.0,0.0)
    bn_exit_qty,  _, _ = summarize_binance_order(bn.get("buy_id"))  if bn.get("buy_id")  else (0.0,0.0,0.0)
    bn_open = round(bn_entry_qty - bn_exit_qty, 8)  # 숏이므로 양수면 열려있는 숏수량

    diff = round(up_open - bn_open, 8)
    if abs(diff) < TRIM_EPS:
        return

    # 업비트 수량 소수 3자리 규격으로 스냅
    if diff > 0:  # 업비트가 더 많다 → 일부 매도
        qty = floor_step(diff, 0.001)
        if qty > 0:
            ok, u_uuid = upbit_order("sell", 0.0, qty)
            if ok and u_uuid:
                _ = _wait_upbit_filled(u_uuid, qty)
                log(f"[트림/UPBIT] -{qty:.3f} BTC (up_open>{bn_open:.3f})")
    else:        # 바이낸스가 더 많다 → 선물 ReduceOnly BUY
        qty = floor_step(-diff, 0.001)
        if qty > 0:
            ok, b_id = binance_order("buy", 0.0, qty, reduce_only=True)
            if ok and b_id:
                _ = _wait_binance_filled(b_id, qty)
                log(f"[트림/BINANCE] +{qty:.3f} BUY RO (bn_open>{up_open:.3f})")

    # 재계산 저장
    band["upbit"]["filled_qty"] = floor_step(max(0.0, up_open - max(0.0,diff)), 0.001) if diff>0 else floor_step(up_open,0.001)
    band["binance"]["entry_qty"] = floor_step(max(0.0, bn_open + min(0.0,diff)), 0.001) if diff>0 else floor_step(bn_open,0.001)
    band["matched_qty"] = floor_step(min(band["upbit"]["filled_qty"], band["binance"]["entry_qty"]), 0.001)

# ---------------- Exit target helper (ADDED) ----------------

def _compute_exit_target_qty(band: Dict[str, Any]) -> float:
    """
    청산 목표 수량을 '밴드 구조량' 중심으로 결정.
    - 기본은 밴드 구조량(nominal_qty)
    - 실제 진입 시 업비트 체결(entry_upbit_qty)과의 괴리는 허용(±), 단위 스냅(0.001)
    - EXIT_MODE에 따라 안전/강경 동작
    """
    nominal = float(band.get("nominal_qty", 0.0))              # 사용자가 의도한 밴드 크기
    up_entry = float(band.get("entry_upbit_qty", 0.0))         # 실제 업비트 진입 체결
    bn_entry = float(band.get("entry_binance_qty", 0.0))       # 실제 바이낸스 진입 체결

    # 기본 목표: 사용자가 의도한 밴드 구조량
    target = nominal if nominal > 0 else min(up_entry, bn_entry)

    # 업비트/바낸의 실제 진입량과 너무 벌어지지 않게(현실적 상한/하한)
    lo = floor_step(min(target, up_entry), 0.001)
    hi = floor_step(max(target, up_entry), 0.001)

    # 목표는 '업비트 실제 진입량'을 우선시하되, 밴드 구조량과의 갭을 허용
    target = max(lo, min(hi, up_entry if up_entry > 0 else target))

    if EXIT_MODE == "by_band_safe":
        # 현재 가용량 계산(초과 방지)
        up = band["upbit"]; bn = band["binance"]
        up_buy_funds, up_buy_vol, _ = summarize_upbit_order(up.get("buy_uuid")) if up.get("buy_uuid") else (0.0,0.0,0.0)
        up_sell_funds, up_sell_vol, _ = summarize_upbit_order(up.get("sell_uuid")) if up.get("sell_uuid") else (0.0,0.0,0.0)
        up_open = max(0.0, round(up_buy_vol - up_sell_vol, 8))
        bn_entry_qty, _, _ = summarize_binance_order(bn.get("sell_id")) if bn.get("sell_id") else (0.0,0.0,0.0)
        bn_exit_qty,  _, _ = summarize_binance_order(bn.get("buy_id"))  if bn.get("buy_id")  else (0.0,0.0,0.0)
        bn_open = max(0.0, round(bn_entry_qty - bn_exit_qty, 8))

        target = floor_step(target, 0.001)
        target = min(target, floor_step(up_open, 0.001))
        target = min(target, floor_step(bn_open, 0.001))
    else:
        # 강경모드: 단위만 맞추고 캡 적용하지 않음(비헤지 가능)
        target = floor_step(target, 0.001)

    return max(0.0, target)

# ---------------- Entry/Exit ----------------

def _compute_auto_size_amount(up: float, bp: float, leverage: int, band: Dict[str, Any]) -> float:
    krw_bal, _ = get_upbit_balance_real()
    usdt_bal   = get_binance_balance_real()
    krw_limit = max(0.0, krw_bal * (1.0 - KRW_BUF_RATIO))
    q_upbit_max = (krw_limit * (1.0 - UPBIT_TAKER_FEE)) / max(1e-9, up)
    usdt_limit = max(0.0, usdt_bal - USDT_FEE_BUFFER)
    q_binance_max = (usdt_limit * leverage) / max(1e-9, bp)
    q_user_cap = float(band.get("max_amount_btc", float("inf")))
    q_cfg_cap  = float(band.get("amount_btc", q_user_cap))
    desired = min(q_upbit_max, q_binance_max, q_user_cap, q_cfg_cap)
    return floor_step(max(0.0, desired), 0.001)


def _exit_now(band: Dict[str, Any]) -> None:
    """
    ✅ 청산 시퀀스(동시): 업비트 매도 + 바이낸스 RO 매수 → 체결 대기 → 잔량 트림/매칭 → 유효 체결 확인 → PnL 확정
    ※ EXIT_MODE에 따라 목표 수량을 밴드 구조량 기반으로 산정
    """
    qty_u = _compute_exit_target_qty(band)
    if qty_u <= 0:
        log(f"[청산 건너뜀/{band['id']}] 목표수량 0 (EXIT_MODE={EXIT_MODE})"); band["state"] = "closed"; return
    log(f"[EXIT-PLAN/{band['id']}] mode={EXIT_MODE} nominal={band.get('nominal_qty')} up_entry={band.get('entry_upbit_qty')} bn_entry={band.get('entry_binance_qty')} target_qty={qty_u:.3f}")

    # 동시 주문 실행 + 밴드에 식별자 저장
    def _sell_upbit(q):
        sold_total, u_uuid = _sell_upbit_exact(q)
        if u_uuid:
            band["upbit"]["sell_uuid"] = u_uuid  # ✅ 저장
            _ = _wait_upbit_filled(u_uuid, sold_total)
        return sold_total

    def _buy_binance(q):
        ok_b, b_id = binance_order("buy", 0.0, q, reduce_only=True)
        if ok_b and b_id:
            band["binance"]["buy_id"] = b_id    # ✅ 저장
            _ = _wait_binance_filled(b_id, q)
        return ok_b

    th1 = threading.Thread(target=_sell_upbit, args=(qty_u,), daemon=True)
    th2 = threading.Thread(target=_buy_binance, args=(qty_u,), daemon=True)
    th1.start(); th2.start(); th1.join(); th2.join()

    # 1차 트림 & 매칭 재산정
    _trim_residual(band)
    _calc_matched_qty(band)

    # 유효 체결 확인(양측 모두)
    up_sold_funds, up_sold_vol, _ = summarize_upbit_order(band["upbit"].get("sell_uuid")) if band["upbit"].get("sell_uuid") else (0.0, 0.0, 0.0)
    bn_exit_qty, _, _ = summarize_binance_order(band["binance"].get("buy_id")) if band["binance"].get("buy_id") else (0.0, 0.0, 0.0)
    qty_close_ok = floor_step(min(up_sold_vol, bn_exit_qty), 0.001)
    if qty_close_ok <= 0:
        log(f"[청산 보류/{band['id']}] 체결 미확인(Upbit_sold={up_sold_vol:.3f}, Bin_exit={bn_exit_qty:.3f})")
        return

    # 확정 PnL 계산/커밋
    pnl = compute_band_pnl_krw(band, get_usdkrw())
    globals()["profit_krw"] += pnl
    globals()["last_realized_pnl_krw"] = pnl

    detail = band.get("last_close_detail", {}) or {}
    recent_closes.append({
        "id": band["id"],
        "pair_id": band.get("pair_id"),
        "ts": time.time(),
        "qty": float(qty_close_ok),
        "entry_kimp": band.get("entry_kimp_value"),
        "exit_kimp": band.get("exit_kimp"),
        "pnl_krw": pnl,
        "upbit_pnl": detail.get("upbit_pnl"),
        "fut_pnl_usdt": detail.get("fut_pnl_usdt"),
        "fx": detail.get("fx"),
        "delta_k_pp": detail.get("delta_k_pp"),
        "notional_krw": detail.get("notional_krw"),
        "delta_k_gain_est": detail.get("delta_k_gain_est"),
    })
    if len(recent_closes) > MAX_RECENT_CLOSES:
        del recent_closes[: len(recent_closes) - MAX_RECENT_CLOSES]

    band["state"] = "closed"
    band["last_close_ts"] = time.time()
    log(f"[청산 완료/{band['id']}] 동시청산, 실현손익={pnl:+,.0f} KRW")

    if band.get("repeat", False):
        cooldown = int(band.get("cooldown_sec", 0))
        log(f"[자동 재무장 예약] id={band['id']} cooldown={cooldown}s")
        threading.Thread(target=_rearm_after_cooldown, args=(band['id'], cooldown), daemon=True).start()


# === 진입 (바이낸스 숏 → 환율×체결가로 업비트 금액매수) ===

def try_enter_band(band: Dict[str, Any], kimp: float, up: float, bp: float) -> None:
    if band["state"] != "waiting": return
    if abs(kimp - band["entry_kimp"]) > band["tolerance"]: return

    leverage = int(band.get("leverage", DEFAULT_LEVERAGE))
    if bool(band.get("auto_size", AUTO_SIZE_DEFAULT)):
        desired_btc = _compute_auto_size_amount(up, bp, leverage, band)
    else:
        desired_btc = float(band.get("amount_btc", 0.0))
    base_qty = floor_step(max(0.0, desired_btc), BN_LOT_STEP)
    base_qty = floor_step(base_qty, 0.001)

    if base_qty <= 0:
        return

    have_usdt = get_binance_balance_real()
    need_usdt_est = (base_qty * bp) / max(1, leverage)
    if have_usdt + 1e-6 < need_usdt_est + USDT_FEE_BUFFER:
        log(f"[진입 중단/{band['id']}] USDT 부족 have≈{have_usdt:.2f}, need≈{need_usdt_est+USDT_FEE_BUFFER:.2f}")
        return

    # 3) 바이낸스 숏 먼저
    try:
        _ = _bn_signed_post("/fapi/v1/order/test",
                            {"symbol":"BTCUSDT","side":"SELL","type":"MARKET","quantity":format(base_qty, ".3f")})
    except Exception as _e:
        log(f"[진입 중단/{band['id']}] Binance test 실패: {_e}")
        return

    ok_b, b_id = binance_order("sell", 0.0, base_qty)
    if not ok_b or not b_id:
        log(f"[진입 실패/{band['id']}] 바이낸스 숏 실패"); return
    band["binance"]["sell_id"] = b_id

    _ = _wait_binance_filled(b_id, base_qty, timeout=3.0)
    bn_qty, bn_avg, _fee = summarize_binance_order(b_id)
    bn_qty = floor_step(bn_qty, BN_LOT_STEP)
    bn_qty = floor_step(bn_qty, 0.001)
    if bn_qty <= 0:
        log(f"[진입 취소/{band['id']}] 바이낸스 체결수량 0"); return

    fx = get_usdkrw()
    krw_amount = bn_avg * bn_qty * fx * UNDERBUY_RATIO
    krw_amount_int = int(math.floor(krw_amount))

    krw_bal, _ = get_upbit_balance_real()
    if krw_bal + 1e-6 < krw_amount_int:
        log(f"[진입 중단/{band['id']}] KRW 부족 have≈{krw_bal:.0f}, need≈{krw_amount_int}")
        return

    ok_u, u_uuid = upbit_order("buy", krw_amount_int, 0.0)
    if not ok_u or not u_uuid:
        log(f"[경고/{band['id']}] 업비트 금액매수 실패 → hedging 상태로 전환")
        band["state"] = "hedging"; band["upbit"]["filled_qty"] = 0.0
        return
    band["upbit"]["buy_uuid"] = u_uuid

    up_funds, up_vol, _ = _wait_upbit_filled(u_uuid, 0.0, timeout=5.0)
    up_vol = round(up_vol, 8)

    if up_vol - bn_qty > 1e-8:
        excess = float(f"{(up_vol - bn_qty):.8f}")
        if excess > 0:
            ok_trim, trim_uuid = upbit_order("sell", 0.0, excess)
            if ok_trim and trim_uuid:
                _ = _wait_upbit_filled(trim_uuid, excess)
            up_vol = bn_qty
            log(f"[잔여 트림/{band['id']}] upbit_excess={excess:.8f} → 맞춤 {up_vol:.8f}")

    band["pair_id"] = uuid.uuid4().hex
    band["state"] = "entered"
    band["entry_kimp_value"] = float(kimp)
    band["entry_ts"] = time.time()

    matched = floor_step(min(up_vol, bn_qty), 0.001)
    band["matched_qty"] = matched
    band["upbit"]["filled_qty"] = matched
    band["binance"]["entry_qty"], band["binance"]["entry_avg"] = bn_qty, bn_avg
    lev = int(band.get("leverage", DEFAULT_LEVERAGE))
    band["binance"]["entry_margin_usdt"] = (bn_qty * bn_avg) / max(1, lev)

    # ▶▶ ADDED: 밴드 구조량/실제 진입 기록(청산 목표 계산용)
    band["nominal_qty"] = float(band.get("amount_btc", base_qty))  # UI에서 설정한 밴드 구조량
    band["entry_upbit_qty"] = round(up_vol, 8)                     # 업비트 실제 체결
    band["entry_binance_qty"] = float(bn_qty)                      # 바이낸스 실제 체결

    _update_slip_ema_from_observed(abs(float(kimp) - float(band["entry_kimp"])))
    log(f"[진입 성공(BN→UP)/{band['id']}] pair={band['pair_id']} bn_qty={bn_qty:.3f} up_qty={up_vol:.8f} matched={matched:.3f} fx={fx:.2f} krw≈{krw_amount_int:,} 김프={kimp}%")

def _sell_upbit_exact(qty: float) -> Tuple[float, Optional[str]]:
    qty = round(max(0.0, float(qty)), 3)
    left = qty; sold_total = 0.0; last_uuid = None
    for _ in range(3):
        if left <= 0: break
        ok, u_uuid = upbit_order("sell", 0.0, left)
        last_uuid = u_uuid if ok else last_uuid
        time.sleep(0.25)
        try:
            _, vol, _ = summarize_upbit_order(u_uuid) if ok and u_uuid else (0.0,0.0,0.0)
        except Exception:
            vol = 0.0
        sold_total += vol
        left = round(qty - sold_total, 3)
    return round(sold_total,3), last_uuid


def _wait_upbit_filled(uuid_str: str, expect_qty: float, timeout: float = 5.0) -> Tuple[float,float,float]:
    t0=time.time(); funds=vol=fee=0.0
    while time.time()-t0 < timeout:
        funds, vol, fee = summarize_upbit_order(uuid_str)
        if vol >= min(expect_qty, vol if vol>0 else expect_qty) - 1e-6 and funds>0:
            break
        time.sleep(0.2)
    return funds, vol, fee


def _wait_binance_filled(order_id: int, expect_qty: float, timeout: float = 5.0) -> Tuple[float,float,float]:
    t0=time.time(); qty=avg=fee=0.0
    while time.time()-t0 < timeout:
        qty, avg, fee = summarize_binance_order(order_id)
        if qty >= min(expect_qty, qty if qty>0 else expect_qty) - 1e-6:
            break
        time.sleep(0.2)
    return qty, avg, fee


def try_exit_band(band: Dict[str, Any], kimp: float) -> None:
    if band["state"] != "entered": return

    # ▶ 진입 직후 즉시청산 방지
    if band.get("entry_ts") and (time.time() - float(band["entry_ts"])) < MIN_HOLD_BEFORE_EXIT_SEC:
        return

    entry = band.get("entry_kimp_value")
    exit_k = float(band["exit_kimp"])
    tp_k   = band.get("tp_kimp")
    sl_k   = band.get("sl_kimp")
    trail  = float(band.get("trail_bp", 0.0))
    be_mov = float(band.get("breakeven_shift_bp", 0.0))

    est_net_bp = None
    if entry is not None:
        est_net_bp = (float(kimp) - float(entry)) - FEES_BP - _current_slip_buffer()

    if entry is not None and be_mov > 0 and est_net_bp is not None and est_net_bp >= be_mov:
        be_line = float(entry) + FEES_BP + _current_slip_buffer()
        if sl_k is None or (exit_k >= float(entry) and be_line > float(sl_k)) or (exit_k < float(entry) and be_line < float(sl_k)):
            band["sl_kimp"] = be_line
            sl_k = be_line

    if entry is not None:
        favorable = (float(kimp) >= float(entry)) if exit_k >= float(entry) else (float(kimp) <= float(entry))
        if favorable:
            pk = band.get("peak_kimp_after_entry")
            if pk is None:
                band["peak_kimp_after_entry"] = float(kimp)
            else:
                if exit_k >= float(entry):
                    if float(kimp) > pk: band["peak_kimp_after_entry"] = float(kimp)
                else:
                    if float(kimp) < pk: band["peak_kimp_after_entry"] = float(kimp)

    if sl_k is not None:
        if (exit_k >= float(entry or exit_k) and kimp <= float(sl_k)) or (exit_k < float(entry or exit_k) and kimp >= float(sl_k)):
            log(f"[손절/{band['id']}] kimp={kimp} sl_kimp={sl_k} est_net={est_net_bp}")
            return _exit_now(band)

    if trail > 0 and band.get("peak_kimp_after_entry") is not None:
        pk = float(band["peak_kimp_after_entry"]); hit=False
        if exit_k >= float(entry or exit_k):
            if pk - float(kimp) >= trail: hit=True
        else:
            if float(kimp) - pk >= trail: hit=True
        if hit:
            log(f"[트레일링 익절/{band['id']}] pk={pk} kimp={kimp} trail={trail}")
            return _exit_now(band)

    def crossed(target, ref):
        return (kimp >= target) if (ref is None or target >= float(ref)) else (kimp <= target)

    target_exit = float(tp_k) if tp_k is not None else exit_k
    if crossed(target_exit, entry):
        if FAST_EXIT_NO_GUARD:
            log(f"[익절(빠름)/{band['id']}] kimp={kimp} target={target_exit}")
            return _exit_now(band)
        if entry is not None:
            min_req = float(band.get("min_net_exit_bp", MIN_NET_EXIT_BP))
            held = time.time() - float(band.get("entry_ts") or time.time())
            if held <= MAX_HOLD_SEC and est_net_bp is not None and est_net_bp + 1e-9 < min_req:
                log(f"[청산 보류/{band['id']}] est_net={est_net_bp:.3f}%p < min_req={min_req:.3f}%p hold={held:.1f}s")
                return
        log(f"[익절/{band['id']}] kimp={kimp} target={target_exit}")
        return _exit_now(band)

# ---------------- Reconciliation Worker ----------------
_reconcile_thread: Optional[threading.Thread] = None


def reconciliation_worker():
    while running:
        try:
            for b in list(bands.values()):
                st = b.get("state")
                if st in ("entered","hedging") or (st=="closed" and abs(float(b.get("upbit",{}).get("filled_qty",0.0)) - float(b.get("binance",{}).get("entry_qty",0.0))) >= TRIM_EPS):
                    _calc_matched_qty(b)
                    _trim_residual(b)
        except Exception as e:
            log(f"[리컨실 워커 오류] {e}")
        time.sleep(RECONCILE_INTERVAL_SEC)

# ---------------- Strategy loop ----------------

def strategy_loop(global_cfg: Dict[str, Any]) -> None:
    global running, last_error, _reconcile_thread

    sync_binance_time()
    set_binance_leverage("BTCUSDT", int(global_cfg.get("leverage", DEFAULT_LEVERAGE)))

    # 리컨실 워커 시작(한 번만)
    if _reconcile_thread is None or not _reconcile_thread.is_alive():
        _reconcile_thread = threading.Thread(target=reconciliation_worker, daemon=True)
        _reconcile_thread.start()

    while running:
        try:
            metrics["loops"] += 1
            kimp, up, bp, fx = calc_kimp()

            for b in list(bands.values()):
                if b["state"] == "waiting":  try_enter_band(b, kimp, up, bp)

            for b in list(bands.values()):
                if b.get("state") == "hedging" and b["upbit"].get("filled_qty",0.0) > 0.0:
                    q = floor_step(b["upbit"].get("filled_qty",0.0), 0.001)
                    ok_b, b_id = binance_order("sell", 0.0, q)
                    if ok_b and b_id:
                        b["binance"]["sell_id"] = b_id
                        b["entry_kimp_value"] = float(kimp)
                        b["entry_ts"] = time.time()
                        b["state"] = "entered"
                        log(f"[헤지 성공/{b['id']}] q={q:.3f} BTC → entered")

            for b in list(bands.values()):
                try_exit_band(b, kimp)

        except Exception as e:
            metrics["api_errors"] += 1; last_error = str(e); log(f"[전략 오류] {e}")

        time.sleep(0.5)

# ---------------- Routes ----------------
@app.route("/")
def index(): return render_template("index.html")

@app.route("/current_kimp")
def current_kimp():
    k, up, bp, fx = calc_kimp()
    fee_bp_est = round(FEES_BP, 2)
    return jsonify({"kimp":k,"upbit_price":up,"binance_price":bp,"usdkrw":fx,
                    "fee_bp_est":fee_bp_est,"required_spread_bp":required_spread_bp()})

@app.route("/balance")
def balance():
    krw, btc = get_upbit_balance_real(); usdt = get_binance_balance_real()
    return jsonify({"real":{"krw":round(krw,0),"btc_upbit":round(btc,6),"usdt":round(usdt,3)}})

@app.route("/status")
def status():
    fx_now = get_usdkrw(); fees_binance_krw_now = fees_binance_usdt_cum * fx_now
    k_now, up_now, bp_now, _ = calc_kimp()

    out_bands = []
    unrealized_total = 0.0
    for b in bands.values():
        unreal_now = 0.0
        if b.get("state") == "entered":
            unreal_now = estimate_unrealized_pnl_krw(b, up_now, bp_now, fx_now)
            unrealized_total += unreal_now

        out_bands.append({
            "id": b["id"], "state": b["state"],
            "pair_id": b.get("pair_id"),
            "entry_kimp": b["entry_kimp"], "exit_kimp": b["exit_kimp"],
            "tolerance": b["tolerance"], "amount_btc": b["amount_btc"],
            "leverage": b["leverage"], "entry_kimp_value": b.get("entry_kimp_value"),
            "filled_qty": b["upbit"].get("filled_qty",0.0),
            "matched_qty": b.get("matched_qty", 0.0),
            "pnl_krw": round(b.get("pnl_krw",0.0),0),
            "last_pnl_krw": round(b.get("last_pnl_krw",0.0),0),
            "unrealized_krw_now": round(unreal_now, 0),
            "repeat": bool(b.get("repeat", False)),
            "cooldown_sec": int(b.get("cooldown_sec", 0)),
            "times_armed": int(b.get("times_armed", 0)),
            "auto_size": bool(b.get("auto_size", AUTO_SIZE_DEFAULT)),
            "max_amount_btc": float(b.get("max_amount_btc", float("inf"))),
            "binance_entry_margin_usdt": round(b.get("binance",{}).get("entry_margin_usdt",0.0), 3),
            "min_net_exit_bp": float(b.get("min_net_exit_bp", MIN_NET_EXIT_BP)),
            "tp_kimp": b.get("tp_kimp"),
            "sl_kimp": b.get("sl_kimp"),
            "trail_bp": float(b.get("trail_bp", 0.0)),
            "breakeven_shift_bp": float(b.get("breakeven_shift_bp", 0.0)),
        })
    return jsonify({
        "running": running, "logs": logs[-350:], "last_error": last_error,
        "binance_testnet": BINANCE_TESTNET, "binance_base": BASE_FAPI,
        "pnl": {
            "profit_krw_cum": round(profit_krw, 0),
            "last_realized_krw": round(last_realized_pnl_krw, 0),
            "unrealized_krw_now": round(unrealized_total, 0),
            "fees_upbit_krw_cum": round(fees_upbit_krw_cum, 0),
            "fees_binance_usdt_cum": round(fees_binance_usdt_cum, 3),
            "fees_binance_krw_cum": round(fees_binance_krw_cum, 0),
            "fees_binance_krw_now": round(fees_binance_krw_now, 0),
        },
        "recent_closes": recent_closes[-MAX_RECENT_CLOSES:],
        "required_spread_bp": required_spread_bp(),
        "bands": out_bands,
    })

# ---- Bands management ----

def _guard_required_spread(entry_kimp: float, exit_kimp: float)->Tuple[bool,str]:
    spread = abs(exit_kimp - entry_kimp)
    req = required_spread_bp()
    if spread + 1e-12 < req:
        return False, (f"Δ김프 {spread:.2f}%p < 최소 요구 {req:.2f}%p"
                       f" (수수료≈{FEES_BP:.2f}%p + 순이익 {TARGET_NET_PROFIT_BP:.2f}%p"
                       f" + 슬리피지≥{_current_slip_buffer():.2f}%p)")
    return True, ""

@app.route("/bands", methods=["GET"])
def bands_list():
    return jsonify({"bands": [b for b in bands.values()]})

@app.route("/bands/add", methods=["POST"])
def bands_add():
    body = request.json or {}
    try:
        entry_kimp = float(body["entry_kimp"])
        exit_kimp  = float(body["exit_kimp"])
        ok,msg = _guard_required_spread(entry_kimp, exit_kimp)
        if not ok: return jsonify({"status":"error","error":msg}), 400

        amount_btc = max(0.001, float(body.get("amount_btc", 0.01)))
        tolerance  = float(body.get("tolerance", DEFAULT_TOLERANCE))
        leverage   = int(float(body.get("leverage", DEFAULT_LEVERAGE)))
        repeat     = bool(body.get("repeat", REPEAT_DEFAULT))
        cooldown   = int(float(body.get("cooldown_sec", REPEAT_COOLDOWN_DEFAULT)))

        auto_size       = bool(body.get("auto_size", AUTO_SIZE_DEFAULT))
        max_amount_btc  = float(body.get("max_amount_btc", amount_btc))
        min_net_exit_bp = float(body.get("min_net_exit_bp", MIN_NET_EXIT_BP))

        tp_kimp = body.get("tp_kimp")
        sl_kimp = body.get("sl_kimp")
        trail_bp = float(body.get("trail_bp", 0.0))
        breakeven_shift_bp = float(body.get("breakeven_shift_bp", 0.0))
    except Exception as e:
        return jsonify({"status":"error","error":f"bad_params: {e}"}), 400

    bid = _new_band_id()
    band = {
        "id": bid, "state": "waiting",
        "entry_kimp": entry_kimp, "exit_kimp": exit_kimp,
        "tolerance": tolerance, "amount_btc": amount_btc, "leverage": leverage,
        "exit_on_sign_change": DEFAULT_EXIT_ON_SIGN_CHANGE,
        "exit_on_move_bp": DEFAULT_EXIT_ON_MOVE_BP,
        "repeat": repeat, "cooldown_sec": cooldown, "times_armed": 0,
        "auto_size": auto_size, "max_amount_btc": max_amount_btc,
        "entry_kimp_value": None, "entry_ts": None,
        "min_net_exit_bp": min_net_exit_bp,
        "tp_kimp": float(tp_kimp) if tp_kimp is not None else None,
        "sl_kimp": float(sl_kimp) if sl_kimp is not None else None,
        "trail_bp": trail_bp,
        "breakeven_shift_bp": breakeven_shift_bp,
        "peak_kimp_after_entry": None,
        "pair_id": None, "matched_qty": 0.0,
        "upbit": {"buy_uuid": None, "sell_uuid": None, "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0},
        "binance": {"sell_id": None, "buy_id": None, "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                    "fee_usdt": 0.0, "entry_margin_usdt": 0.0},
        "pnl_krw": 0.0,
    }
    bands[bid] = band
    log(f"[밴드 추가] id={bid} entry={entry_kimp} exit={exit_kimp} tol=±{tolerance} amount={amount_btc}BTC lev=x{leverage} repeat={repeat} cd={cooldown} auto_size={auto_size} max_amount_btc={max_amount_btc} min_net_exit_bp={min_net_exit_bp} (최소요구 {required_spread_bp():.2f}%p)")
    return jsonify({"status":"ok","id":bid})

@app.route("/bands/update_repeat", methods=["POST"])
def bands_update_repeat():
    body = request.json or {}; bid = str(body.get("id",""))
    b = bands.get(bid)
    if not b: return jsonify({"status":"error","error":"no_such_band"}), 404
    if "repeat" in body: b["repeat"] = bool(body["repeat"])
    if "cooldown_sec" in body: b["cooldown_sec"] = int(float(body["cooldown_sec"]))
    log(f"[밴드 재무장 갱신] id={bid} repeat={b.get('repeat')} cooldown={b.get('cooldown_sec')}")
    return jsonify({"status":"ok"})

@app.route("/bands/cancel", methods=["POST"])
def bands_cancel():
    body = request.json or {}; bid = str(body.get("id",""))
    b = bands.get(bid)
    if not b: return jsonify({"status":"error","error":"no_such_band"}), 404
    if b["state"] != "waiting":
        return jsonify({"status":"error","error":"cannot_cancel_after_entry"}), 409
    b["state"] = "cancelled"; log(f"[밴드 취소] id={bid}")
    return jsonify({"status":"ok"})

@app.route("/bands/force_exit", methods=["POST"])
def bands_force_exit():
    body = request.json or {}; bid = str(body.get("id",""))
    b = bands.get(bid)
    if not b: return jsonify({"status":"error","error":"no_such_band"}), 404
    if b["state"] != "entered":
        return jsonify({"status":"error","error":"band_not_entered"}), 409
    # 강제 청산은 즉시 실행(보유시간 가드 무시)
    _exit_now(b)
    return jsonify({"status":"ok"})

@app.route("/bands/clear", methods=["POST"])
def bands_clear():
    remove_ids = [k for k,v in bands.items() if v["state"] in ("closed","cancelled") and not v.get("repeat", False)]
    for k in remove_ids: bands.pop(k, None)
    log(f"[밴드 정리] removed={remove_ids}")
    return jsonify({"status":"ok","removed":remove_ids})

# ---- Engine control ----
@app.route("/start", methods=["POST"])
def start():
    global running, last_error
    if not running:
        keys = load_api_keys()
        if not keys.get("binance_key") or not keys.get("binance_secret"):
            last_error="binance_invalid_key_missing"; log("[전략 시작 실패] BINANCE 키 누락")
            return jsonify({"status":"error","error":last_error}), 400
        cfg_in = request.json or {}
        if "target_kimp" in cfg_in or "entry_kimp" in cfg_in:
            entry_k = float(cfg_in.get("entry_kimp", cfg_in.get("target_kimp", 0.0)))
            exit_k  = float(cfg_in.get("exit_kimp", 0.3))
            ok,msg = _guard_required_spread(entry_k, exit_k)
            if not ok: return jsonify({"status":"error","error":msg}), 400

            amount  = float(cfg_in.get("amount_btc", 0.01))
            tol     = float(cfg_in.get("tolerance", DEFAULT_TOLERANCE))
            lev     = int(float(cfg_in.get("leverage", DEFAULT_LEVERAGE)))
            repeat  = bool(cfg_in.get("repeat", REPEAT_DEFAULT))
            cooldown= int(float(cfg_in.get("cooldown_sec", REPEAT_COOLDOWN_DEFAULT)))
            auto_size = bool(cfg_in.get("auto_size", AUTO_SIZE_DEFAULT))
            max_amount_btc = float(cfg_in.get("max_amount_btc", amount))
            min_net_exit_bp = float(cfg_in.get("min_net_exit_bp", MIN_NET_EXIT_BP))
            tp_kimp = cfg_in.get("tp_kimp")
            sl_kimp = cfg_in.get("sl_kimp")
            trail_bp = float(cfg_in.get("trail_bp", 0.0))
            breakeven_shift_bp = float(cfg_in.get("breakeven_shift_bp", 0.0))

            bid = _new_band_id()
            bands[bid] = {
                "id": bid, "state": "waiting",
                "entry_kimp": entry_k, "exit_kimp": exit_k, "tolerance": tol,
                "amount_btc": amount, "leverage": lev,
                "exit_on_sign_change": DEFAULT_EXIT_ON_SIGN_CHANGE,
                "exit_on_move_bp": DEFAULT_EXIT_ON_MOVE_BP,
                "repeat": repeat, "cooldown_sec": cooldown, "times_armed": 0,
                "auto_size": auto_size, "max_amount_btc": max_amount_btc,
                "entry_kimp_value": None, "entry_ts": None,
                "min_net_exit_bp": min_net_exit_bp,
                "tp_kimp": float(tp_kimp) if tp_kimp is not None else None,
                "sl_kimp": float(sl_kimp) if sl_kimp is not None else None,
                "trail_bp": trail_bp,
                "breakeven_shift_bp": breakeven_shift_bp,
                "peak_kimp_after_entry": None,
                "pair_id": None, "matched_qty": 0.0,
                "upbit": {"buy_uuid": None, "sell_uuid": None, "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0},
                "binance": {"sell_id": None, "buy_id": None, "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                            "fee_usdt": 0.0, "entry_margin_usdt": 0.0},
                "pnl_krw": 0.0,
            }
            log(f"[밴드 추가(start)] entry={entry_k} exit={exit_k} amount={amount}BTC tol=±{tol} lev=x{lev} repeat={repeat} cd={cooldown} auto_size={auto_size} max_amount_btc={max_amount_btc} min_net_exit_bp={min_net_exit_bp} (최소요구 {required_spread_bp():.2f}%p)")
        running=True
        cfg = {"leverage": int(float(cfg_in.get("leverage", DEFAULT_LEVERAGE)))}
        threading.Thread(target=strategy_loop, args=(cfg,), daemon=True).start()
        log(f"[전략 시작] testnet={BINANCE_TESTNET} base={BASE_FAPI}")
    return jsonify({"status":"started"})

@app.route("/stop", methods=["POST"]) 
def stop():
    global running
    running=False; log("[전략 중지]"); return jsonify({"status":"stopped"})

@app.route("/health")
def health(): return jsonify({"thread_alive": bool(running)})

@app.route("/metrics")
def metrics_route(): return jsonify(metrics)

if __name__ == "__main__":
    sync_binance_time()
    app.run(host="0.0.0.0", port=5000, debug=False)
