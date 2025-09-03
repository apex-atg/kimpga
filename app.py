# app_multi_band.py — Upbit Spot LONG + Binance Futures SHORT
# Multi-Band / Queue + Auto Re-Arm, exit_kimp ONLY close, min-net-profit guard, auto sizing
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

# 목표 순이익(+0.10%p)과 슬리피지 버퍼(기본 0.05%p)를 합쳐 최소 폭을 강제
TARGET_NET_PROFIT_BP = float(os.getenv("TARGET_NET_PROFIT_BP","0.10"))  # %p
SLIPPAGE_BP_BUFFER   = float(os.getenv("SLIPPAGE_BP_BUFFER","0.05"))    # %p
FEES_BP = (UPBIT_TAKER_FEE*2 + BINANCE_TAKER_FEE*2) * 100.0             # ≈0.18%p
REQUIRED_SPREAD_BP = round(FEES_BP + TARGET_NET_PROFIT_BP + SLIPPAGE_BP_BUFFER, 4)

def floor_step(x: float, step: float = 0.001) -> float:
    return math.floor(float(x)/step)*step

# ---------------- Auto sizing ----------------
AUTO_SIZE_DEFAULT = True                       # 기본: 자동 사이징 켜기
KRW_BUF_RATIO     = float(os.getenv("KRW_BUF_RATIO","0.001"))   # 0.1% 버퍼
USDT_FEE_BUFFER   = float(os.getenv("USDT_FEE_BUFFER","1.0"))   # 수수료/슬리피지 대비 USDT 버퍼

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

# 밴드 큐
# state: waiting → hedging → entered → closed / cancelled
bands: Dict[str, Dict[str, Any]] = {}

# 기본 옵션
DEFAULT_TOLERANCE = float(os.getenv("DEFAULT_TOLERANCE","0.10"))
DEFAULT_LEVERAGE  = int(float(os.getenv("DEFAULT_LEVERAGE","3")))
# === 조기 트리거 제거(기본값 OFF) ===
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
            elif b["currency"]=="BTC": btc=float(b["balance"]) + float(b["locked"])  # 체결대기 포함
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
        log(f"[바이낸스 주문] {side.upper()} qty={params['quantity']} RO={reduce_only} → {r.status_code} {data}")
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
    band["entry_kimp_value"] = None
    band["upbit"] = {"buy_uuid": None, "sell_uuid": None,
                     "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0}
    band["binance"] = {"sell_id": None, "buy_id": None,
                       "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                       "fee_usdt": 0.0, "entry_margin_usdt": 0.0}

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
    global fees_upbit_krw_cum, fees_binance_usdt_cum, fees_binance_krw_cum

    up = band["upbit"]; bn = band["binance"]

    # --- Upbit: 총액으로 갱신, 누적은 델타만 ---
    buy_funds = buy_vol = buy_fee = 0.0
    if up["buy_uuid"]:
        buy_funds, buy_vol, buy_fee = summarize_upbit_order(up["buy_uuid"])
    sell_funds = sell_fee = 0.0
    if up.get("sell_uuid"):
        sell_funds, _, sell_fee = summarize_upbit_order(up["sell_uuid"])

    prev_up_fee = up.get("fee_krw", 0.0)
    up["buy_krw"] = buy_funds
    up["sell_krw"] = sell_funds
    up["filled_qty"] = max(up.get("filled_qty",0.0), buy_vol)
    up["fee_krw"] = float(buy_fee + sell_fee)
    fees_upbit_krw_cum += max(0.0, up["fee_krw"] - prev_up_fee)

    upbit_pnl = up["sell_krw"] - up["buy_krw"] - up["fee_krw"]

    # --- Binance: 총액으로 갱신, 누적은 델타만 ---
    entry_qty = entry_avg = entry_fee = 0.0
    if bn.get("sell_id"):
        entry_qty, entry_avg, entry_fee = summarize_binance_order(bn["sell_id"])
    exit_qty = exit_avg = exit_fee = 0.0
    if bn.get("buy_id"):
        exit_qty, exit_avg, exit_fee = summarize_binance_order(bn["buy_id"])

    prev_bn_fee = bn.get("fee_usdt", 0.0)
    bn["entry_qty"], bn["entry_avg"] = entry_qty, entry_avg
    bn["exit_qty"],  bn["exit_avg"]  = exit_qty,  exit_avg
    bn["fee_usdt"] = float(entry_fee + exit_fee)

    fee_delta = max(0.0, bn["fee_usdt"] - prev_bn_fee)
    fees_binance_usdt_cum += fee_delta
    fees_binance_krw_cum  += fee_delta * fx_for_exit

    qty_close = min(entry_qty, exit_qty)
    fut_pnl_usdt = (entry_avg - exit_avg) * qty_close - bn["fee_usdt"]

    total_krw = upbit_pnl + fut_pnl_usdt * fx_for_exit
    band["pnl_krw"] = total_krw
    band["last_pnl_krw"] = total_krw
    log(f"[실현손익/{band['id']}] {total_krw:+,.0f} KRW (Upbit {upbit_pnl:+,.0f} + Bin {fut_pnl_usdt:+.3f}USDT)")
    return total_krw

# ---------------- Entry/Exit ----------------
def _compute_auto_size_amount(up: float, bp: float, leverage: int, band: Dict[str, Any]) -> float:
    # 잔고 기반 최대 q 계산
    krw_bal, _ = get_upbit_balance_real()
    usdt_bal   = get_binance_balance_real()

    # 업비트: gross_krw = q*up / (1 - fee) ≤ krw_bal*(1 - KRW_BUF_RATIO)
    krw_limit = max(0.0, krw_bal * (1.0 - KRW_BUF_RATIO))
    q_upbit_max = (krw_limit * (1.0 - UPBIT_TAKER_FEE)) / max(1e-9, up)

    # 바이낸스: margin = q*bp / L ≤ usdt_bal - USDT_FEE_BUFFER
    usdt_limit = max(0.0, usdt_bal - USDT_FEE_BUFFER)
    q_binance_max = (usdt_limit * leverage) / max(1e-9, bp)

    q_user_cap = float(band.get("max_amount_btc", float("inf")))
    q_cfg_cap  = float(band.get("amount_btc", q_user_cap))
    desired = min(q_upbit_max, q_binance_max, q_user_cap, q_cfg_cap)
    return floor_step(max(0.0, desired), 0.001)

def try_enter_band(band: Dict[str, Any], kimp: float, up: float, bp: float) -> None:
    if band["state"] != "waiting": return
    if abs(kimp - band["entry_kimp"]) > band["tolerance"]: return

    leverage = int(band.get("leverage", DEFAULT_LEVERAGE))
    if bool(band.get("auto_size", AUTO_SIZE_DEFAULT)):
        desired_btc = _compute_auto_size_amount(up, bp, leverage, band)
    else:
        desired_btc = floor_step(float(band.get("amount_btc", 0.0)), 0.001)
    if desired_btc <= 0: return

    # 업비트 예산 체크 + 소액 버퍼
    gross_krw = desired_btc * up / max(1e-9, (1.0 - UPBIT_TAKER_FEE))
    gross_krw *= (1.0 + KRW_BUF_RATIO)
    gross_krw_int = int(math.ceil(gross_krw))
    krw_bal, _ = get_upbit_balance_real()
    if krw_bal + 1e-6 < gross_krw_int:
        log(f"[진입 중단/{band['id']}] KRW 부족 have≈{krw_bal:.0f}, need≈{gross_krw_int}")
        return

    # 바이낸스 예산 체크(버퍼 포함)
    need_usdt_est = (desired_btc * bp) / max(1, leverage)
    have_usdt = get_binance_balance_real()
    if have_usdt + 1e-6 < need_usdt_est + USDT_FEE_BUFFER:
        log(f"[진입 중단/{band['id']}] USDT 부족 have≈{have_usdt:.2f}, need≈{need_usdt_est+USDT_FEE_BUFFER:.2f}")
        return

    # 테스트 주문
    try:
        _ = _bn_signed_post("/fapi/v1/order/test",
                            {"symbol":"BTCUSDT","side":"SELL","type":"MARKET",
                             "quantity":format(desired_btc, ".3f")})
    except Exception as _e:
        log(f"[진입 중단/{band['id']}] Binance test 실패: {_e}")
        return

    # --- 실제 체결 ---
    ok_u, u_uuid = upbit_order("buy", gross_krw_int, desired_btc)
    if not ok_u or not u_uuid:
        log(f"[진입 실패/{band['id']}] 업비트 매수 실패"); return
    band["upbit"]["buy_uuid"] = u_uuid

    # 업비트 체결 수량을 그대로 바이낸스 숏 수량으로 사용 (사이즈 매칭)
    filled_qty = 0.0
    for _ in range(8):
        time.sleep(0.25)
        _, vol, _ = summarize_upbit_order(u_uuid)
        if vol > 0: filled_qty = vol; break
    if filled_qty <= 0: filled_qty = desired_btc

    binance_qty = floor_step(filled_qty, 0.001)
    if binance_qty <= 0:
        band["state"] = "hedging"; band["upbit"]["filled_qty"] = 0.0
        log(f"[진입 보류/{band['id']}] 체결 수량 0 → hedging"); return

    ok_b, b_id = binance_order("sell", 0.0, binance_qty)
    if not ok_b or not b_id:
        log(f"[헤지 대기/{band['id']}] 바이낸스 숏 실패 → hedging")
        band["state"] = "hedging"; band["upbit"]["filled_qty"] = round(filled_qty, 3); return

    band["state"] = "entered"
    band["entry_kimp_value"] = float(kimp)
    band["upbit"]["filled_qty"] = round(filled_qty, 3)
    band["binance"]["sell_id"] = b_id

    # ▼ 진입 체결 요약 → 진입 증거금(USDT) 저장
    q,a,f = summarize_binance_order(b_id)
    lev = int(band.get("leverage", DEFAULT_LEVERAGE))
    band["binance"]["entry_qty"], band["binance"]["entry_avg"] = q, a
    band["binance"]["entry_margin_usdt"] = (q * a) / max(1, lev)

    log(f"[진입 성공/{band['id']}] ~{filled_qty:.3f} BTC (auto_size={band.get('auto_size', AUTO_SIZE_DEFAULT)}) / 김프 {kimp}% @entry {band['entry_kimp']}±{band['tolerance']}")

def _sell_upbit_exact(qty: float) -> Tuple[float, Optional[str]]:
    """업비트 시장가로 qty 전량을 최대 3회에 나눠 강제 매도. (잔량 0 보장 시도)"""
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

# ▼ 체결 완료 대기 유틸
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

    entry = band.get("entry_kimp_value")
    exit_k = band["exit_kimp"]

    # === 오직 exit_kimp 도달만 사용 ===
    crossed = (kimp >= exit_k) if (entry is None or exit_k >= float(entry)) else (kimp <= exit_k)
    if not crossed: return

    log(f"[청산 트리거/{band['id']}:exit_kimp] entry={entry}, now={kimp}")

    qty_u = round(float(band["upbit"].get("filled_qty",0.0)), 3)
    if qty_u <= 0:
        log(f"[청산 건너뜀/{band['id']}] 보유수량 0"); band["state"] = "closed"; return

    # 업비트: 잔량 0이 될 때까지 재매도
    sold_total, u_uuid = _sell_upbit_exact(qty_u)
    if u_uuid:
        band["upbit"]["sell_uuid"] = u_uuid
        _ = _wait_upbit_filled(u_uuid, sold_total)  # ✅ 체결 완료 대기

    # 바이낸스: 업비트 매도된 '실제 수량'만큼 reduceOnly 매수
    if sold_total > 0:
        ok_b, b_id = binance_order("buy", 0.0, sold_total, reduce_only=True)
        if ok_b and b_id:
            band["binance"]["buy_id"] = b_id
            _ = _wait_binance_filled(b_id, sold_total)  # ✅ 체결 완료 대기

    pnl = compute_band_pnl_krw(band, get_usdkrw())
    globals()["profit_krw"] += pnl

    band["state"] = "closed"
    band["last_close_ts"] = time.time()
    log(f"[청산 완료/{band['id']}] 상태=closed, 실현손익={pnl:+,.0f} KRW")

    # 자동 재무장
    if band.get("repeat", False):
        cooldown = int(band.get("cooldown_sec", 0))
        log(f"[자동 재무장 예약] id={band['id']} cooldown={cooldown}s")
        threading.Thread(target=_rearm_after_cooldown, args=(band["id"], cooldown), daemon=True).start()

# ---------------- Strategy loop ----------------
def strategy_loop(global_cfg: Dict[str, Any]) -> None:
    global running, last_error

    sync_binance_time()
    set_binance_leverage("BTCUSDT", int(global_cfg.get("leverage", DEFAULT_LEVERAGE)))

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
                    "fee_bp_est":fee_bp_est,"required_spread_bp":REQUIRED_SPREAD_BP})

@app.route("/balance")
def balance():
    krw, btc = get_upbit_balance_real(); usdt = get_binance_balance_real()
    return jsonify({"real":{"krw":round(krw,0),"btc_upbit":round(btc,6),"usdt":round(usdt,3)}})

@app.route("/status")
def status():
    fx_now = get_usdkrw(); fees_binance_krw_now = fees_binance_usdt_cum * fx_now
    out_bands = []
    for b in bands.values():
        out_bands.append({
            "id": b["id"], "state": b["state"],
            "entry_kimp": b["entry_kimp"], "exit_kimp": b["exit_kimp"],
            "tolerance": b["tolerance"], "amount_btc": b["amount_btc"],
            "leverage": b["leverage"], "entry_kimp_value": b.get("entry_kimp_value"),
            "filled_qty": b["upbit"].get("filled_qty",0.0),
            "pnl_krw": round(b.get("pnl_krw",0.0),0),
            "repeat": bool(b.get("repeat", False)),
            "cooldown_sec": int(b.get("cooldown_sec", 0)),
            "times_armed": int(b.get("times_armed", 0)),
            "last_pnl_krw": round(b.get("last_pnl_krw",0.0),0),
            "auto_size": bool(b.get("auto_size", AUTO_SIZE_DEFAULT)),
            "max_amount_btc": float(b.get("max_amount_btc", float("inf"))),
            "binance_entry_margin_usdt": round(b.get("binance",{}).get("entry_margin_usdt",0.0), 3),
        })
    return jsonify({
        "running": running, "logs": logs[-350:], "last_error": last_error,
        "binance_testnet": BINANCE_TESTNET, "binance_base": BASE_FAPI,
        "pnl": {
            "profit_krw_cum": round(profit_krw, 0),
            "fees_upbit_krw_cum": round(fees_upbit_krw_cum, 0),
            "fees_binance_usdt_cum": round(fees_binance_usdt_cum, 3),
            "fees_binance_krw_cum": round(fees_binance_krw_cum, 0),
            "fees_binance_krw_now": round(fees_binance_krw_now, 0),
        },
        "required_spread_bp": REQUIRED_SPREAD_BP,
        "bands": out_bands,
    })

# ---- Bands management ----
def _guard_required_spread(entry_kimp: float, exit_kimp: float)->Tuple[bool,str]:
    spread = abs(exit_kimp - entry_kimp)
    if spread + 1e-12 < REQUIRED_SPREAD_BP:
        return False, (f"Δ김프 {spread:.2f}%p < 최소 요구 {REQUIRED_SPREAD_BP:.2f}%p"
                       f" (수수료≈{FEES_BP:.2f}%p + 순이익 {TARGET_NET_PROFIT_BP:.2f}%p"
                       f" + 슬리피지 {SLIPPAGE_BP_BUFFER:.2f}%p)")
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

        amount_btc = max(0.001, float(body.get("amount_btc", 0.01)))  # 상한(또는 기본)
        tolerance  = float(body.get("tolerance", DEFAULT_TOLERANCE))
        leverage   = int(float(body.get("leverage", DEFAULT_LEVERAGE)))
        repeat     = bool(body.get("repeat", REPEAT_DEFAULT))
        cooldown   = int(float(body.get("cooldown_sec", REPEAT_COOLDOWN_DEFAULT)))

        # auto sizing
        auto_size       = bool(body.get("auto_size", AUTO_SIZE_DEFAULT))
        max_amount_btc  = float(body.get("max_amount_btc", amount_btc))  # 사용자 상한
    except Exception as e:
        return jsonify({"status":"error","error":f"bad_params: {e}"}), 400

    bid = _new_band_id()
    band = {
        "id": bid, "state": "waiting",
        "entry_kimp": entry_kimp, "exit_kimp": exit_kimp,
        "tolerance": tolerance, "amount_btc": amount_btc, "leverage": leverage,
        "exit_on_sign_change": DEFAULT_EXIT_ON_SIGN_CHANGE,  # 비활성
        "exit_on_move_bp": DEFAULT_EXIT_ON_MOVE_BP,          # 비활성
        "repeat": repeat, "cooldown_sec": cooldown, "times_armed": 0,
        "auto_size": auto_size, "max_amount_btc": max_amount_btc,
        "entry_kimp_value": None,
        "upbit": {"buy_uuid": None, "sell_uuid": None, "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0},
        "binance": {"sell_id": None, "buy_id": None, "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                    "fee_usdt": 0.0, "entry_margin_usdt": 0.0},
        "pnl_krw": 0.0,
    }
    bands[bid] = band
    log(f"[밴드 추가] id={bid} entry={entry_kimp} exit={exit_kimp} tol=±{tolerance} "
        f"amount={amount_btc}BTC lev=x{leverage} repeat={repeat} cd={cooldown} "
        f"auto_size={auto_size} max_amount_btc={max_amount_btc} "
        f"(최소요구 {REQUIRED_SPREAD_BP:.2f}%p)")
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
    try_exit_band(b, calc_kimp()[0])
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
            last_error="binance_api_key_missing"; log("[전략 시작 실패] BINANCE 키 누락")
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
            bid = _new_band_id()
            bands[bid] = {
                "id": bid, "state": "waiting",
                "entry_kimp": entry_k, "exit_kimp": exit_k, "tolerance": tol,
                "amount_btc": amount, "leverage": lev,
                "exit_on_sign_change": DEFAULT_EXIT_ON_SIGN_CHANGE,
                "exit_on_move_bp": DEFAULT_EXIT_ON_MOVE_BP,
                "repeat": repeat, "cooldown_sec": cooldown, "times_armed": 0,
                "auto_size": auto_size, "max_amount_btc": max_amount_btc,
                "entry_kimp_value": None,
                "upbit": {"buy_uuid": None, "sell_uuid": None, "buy_krw": 0.0, "sell_krw": 0.0, "fee_krw": 0.0, "filled_qty": 0.0},
                "binance": {"sell_id": None, "buy_id": None, "entry_qty": 0.0, "entry_avg": 0.0, "exit_qty": 0.0, "exit_avg": 0.0,
                            "fee_usdt": 0.0, "entry_margin_usdt": 0.0},
                "pnl_krw": 0.0,
            }
            log(f"[밴드 추가(start)] entry={entry_k} exit={exit_k} amount={amount}BTC tol=±{tol} "
                f"lev=x{lev} repeat={repeat} cd={cooldown} auto_size={auto_size} max_amount_btc={max_amount_btc} "
                f"(최소요구 {REQUIRED_SPREAD_BP:.2f}%p)")
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
