# app.py — Multi-Leg Stack / Flip Mode (Upbit Spot LONG + Binance Futures SHORT)
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

app = Flask(__name__)
CORS(app)

# ---------- robust HTTP ----------
def _make_session() -> requests.Session:
    s = requests.Session()
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=(408,429,500,502,503,504),
                  allowed_methods=frozenset(["GET","POST"]),
                  raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", ad); s.mount("http://", ad)
    return s
HTTP = _make_session()
TIMEOUT = (3, 7)

# ---------- global state ----------
state_lock = threading.Lock()
running: bool = False
trade_count: int = 0
position_state: str = "neutral"

profit_krw: float = 0.0
fees_upbit_krw_cum: float = 0.0
fees_binance_usdt_cum: float = 0.0
fees_binance_krw_cum: float = 0.0

logs: List[str] = []
last_error: Optional[str] = None

# 집계/레그
entry_info: Dict[str, float] = {"upbit_qty": 0.0, "binance_qty": 0.0}
next_leg_id: int = 1
open_legs: List[Dict[str, Any]] = []
closed_legs_recent: List[Dict[str, Any]] = []

# 메트릭
metrics = {"loops":0, "orders_binance":0, "orders_upbit":0, "api_errors":0}

# 현재 실행 중 설정(플립 프리셋 포함)
last_cfg: Dict[str, Any] = {}

def log(msg: str) -> None:
    ts = time.strftime("[%H:%M:%S]"); logs.append(f"{ts} {msg}")
    if len(logs) > 500: del logs[:200]

# ---------- Binance base ----------
BINANCE_TESTNET = str(os.getenv("BINANCE_TESTNET","false")).lower() in ("1","true","yes")
BASE_FAPI = "https://testnet.binancefuture.com" if BINANCE_TESTNET else "https://fapi.binance.com"
RECV_WINDOW = 5000
_MAX_TS_RETRY = 1
_bn_time_offset_ms = 0

def _bn_headers() -> dict:
    k = load_api_keys()
    return {"X-MBX-APIKEY": k.get("binance_key",""),
            "Content-Type":"application/x-www-form-urlencoded"}

def _bn_now_ms() -> int: return int(time.time()*1000)+_bn_time_offset_ms

def _bn_sign(params: Dict[str, Any]) -> str:
    k = load_api_keys(); sec = k.get("binance_secret","")
    qs = urlencode(params, doseq=True)
    sig = hmac.new(sec.encode(), qs.encode(), hashlib.sha256).hexdigest()
    return qs+"&signature="+sig

def _is_ts_error(t: str) -> bool:
    t = (t or "").lower()
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
        _bn_time_offset_ms = 0; log(f"[시간동기 실패] {e}")

def _bn_signed_get(path, params=None, retry=_MAX_TS_RETRY):
    params = params or {}; params["timestamp"]=_bn_now_ms(); params["recvWindow"]=RECV_WINDOW
    url = f"{BASE_FAPI}{path}?{_bn_sign(params)}"
    r = HTTP.get(url, headers=_bn_headers(), timeout=TIMEOUT)
    if r.status_code==400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021(GET) → 재동기"); sync_binance_time(); return _bn_signed_get(path, params, retry-1)
    return r

def _bn_signed_post(path, params=None, retry=_MAX_TS_RETRY):
    params = params or {}; params["timestamp"]=_bn_now_ms(); params["recvWindow"]=RECV_WINDOW
    r = HTTP.post(f"{BASE_FAPI}{path}", headers=_bn_headers(), data=_bn_sign(params), timeout=TIMEOUT)
    if r.status_code==400 and retry>0 and _is_ts_error(r.text):
        log("[경고] -1021(POST) → 재동기"); sync_binance_time(); return _bn_signed_post(path, params, retry-1)
    return r

# ---------- prices & fx ----------
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
            _FX_CACHE_TS, _FX_CACHE_VAL = now, fx; return fx
    except Exception as e: log(f"[FX] 구글 실패:{e} → KRW-USDT")
    try:
        r=HTTP.get("https://api.upbit.com/v1/ticker?markets=KRW-USDT", timeout=TIMEOUT); r.raise_for_status()
        fx=float(r.json()[0]["trade_price"]); _FX_CACHE_TS,_FX_CACHE_VAL=now,fx; return fx
    except Exception as e: log(f"[FX] 폴백 실패:{e}")
    _FX_CACHE_TS,_FX_CACHE_VAL=now,1300.0; return 1300.0
def calc_kimp()->Tuple[float,float,float,float]:
    up, bp, fx = get_upbit_price(), get_binance_price(), get_usdkrw()
    k=((up-bp*fx)/(bp*fx))*100.0
    return round(k,2), up, bp, fx

# ---------- balances ----------
def get_upbit_balance_real()->Tuple[float,float]:
    k=load_api_keys(); access_key=k.get("upbit_key",""); secret_key=k.get("upbit_secret","")
    payload={"access_key":access_key,"nonce":str(uuid.uuid4())}
    jwt_token=jwt.encode(payload, secret_key, algorithm="HS256")
    try:
        r=HTTP.get("https://api.upbit.com/v1/accounts", headers={"Authorization":f"Bearer {jwt_token}"}, timeout=TIMEOUT)
        r.raise_for_status(); krw=btc=0.0
        for b in r.json():
            if b["currency"]=="KRW": krw=float(b["balance"])
            elif b["currency"]=="BTC": btc=float(b["balance"])+float(b["locked"])
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

# ---------- orders & fills ----------
def set_binance_leverage(symbol:str, leverage:int)->bool:
    try:
        r=_bn_signed_post("/fapi/v1/leverage",{"symbol":symbol,"leverage":leverage})
        log(f"[레버리지] {symbol} x{leverage} → {r.status_code}")
        if r.status_code!=200 and '"code":-2015' in r.text: globals()["last_error"]="binance_invalid_key_or_permission (-2015)"
        return r.status_code==200
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[레버리지 오류]{e}"); return False

def upbit_order(side:str, price:float, volume:float)->Tuple[bool,Optional[str]]:
    k=load_api_keys(); ak=k.get("upbit_key",""); sk=k.get("upbit_secret","")
    if side=="buy":
        q={"market":"KRW-BTC","side":"bid","ord_type":"price","price":str(int(price))}
    else:
        q={"market":"KRW-BTC","side":"ask","ord_type":"market","volume":format(float(volume),".8f")}
    m=hashlib.sha512(); m.update(urlencode(q).encode())
    payload={"access_key":ak,"nonce":str(uuid.uuid4()),"query_hash":m.hexdigest(),"query_hash_alg":"SHA512"}
    jwt_token=jwt.encode(payload, sk, algorithm="HS256")
    r=HTTP.post("https://api.upbit.com/v1/orders", params=q, headers={"Authorization":f"Bearer {jwt_token}"}, timeout=TIMEOUT)
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
    r=HTTP.get("https://api.upbit.com/v1/order", headers={"Authorization":f"Bearer {jwt_token}"}, params=q, timeout=TIMEOUT)
    return r.json() if r.status_code==200 else {"error":r.text}

def summarize_upbit_order(uuid_str:str)->Tuple[float,float,float,float]:
    d=upbit_order_detail(uuid_str)
    vol=0.0; funds=0.0; fee_krw=0.0
    trades=d.get("trades",[]) or []
    if trades:
        for t in trades:
            v=float(t.get("volume","0") or 0.0)
            f=float(t.get("funds","0") or 0.0)
            fee=float(t.get("fee","0") or 0.0)
            fc=t.get("fee_currency","KRW")
            px=float(t.get("price","0") or 0.0)
            vol+=v; funds+=f
            fee_krw += fee * (px if fc=="BTC" else 1.0)
    else:
        vol=float(d.get("executed_volume","0") or 0.0)
        paid_fee=float(d.get("paid_fee","0") or 0.0)
        side=d.get("side","bid")
        px=float(d.get("price","0") or get_upbit_price())
        fee_krw = paid_fee * (px if side=="ask" and paid_fee<1 else 1.0)
        funds = px * vol
    avg=(funds/vol) if vol>0 else 0.0
    return funds, vol, fee_krw, avg

def get_binance_position_qty()->float:
    r=_bn_signed_get("/fapi/v2/positionRisk")
    if r.status_code==200:
        for p in r.json():
            if p.get("symbol")=="BTCUSDT":
                try: return float(p.get("positionAmt",0.0))
                except Exception: return 0.0
    return 0.0

def binance_order(side:str, price:float, volume:float, reduce_only:bool=False)->Tuple[bool,Optional[int]]:
    try:
        params={"symbol":"BTCUSDT","side":"SELL" if side=="sell" else "BUY","type":"MARKET","quantity":format(float(volume),".3f")}
        if reduce_only: params["reduceOnly"]="true"
        r=_bn_signed_post("/fapi/v1/order", params)
        try: data=r.json()
        except Exception: data={"raw":r.text}
        log(f"[바이낸스 주문] {side.upper()} qty={params['quantity']} RO={reduce_only} → {r.status_code}")
        if r.status_code==200 and isinstance(data,dict) and "orderId" in data:
            metrics["orders_binance"]+=1; return True, int(data["orderId"])
        if r.status_code!=200 and '"code":-2015' in r.text: globals()["last_error"]="binance_invalid_key_or_permission (-2015)"
        return False, None
    except Exception as e:
        metrics["api_errors"]+=1; log(f"[바이낸스 주문 오류]{e}"); return False, None

def binance_user_trades(order_id:int)->List[dict]:
    r=_bn_signed_get("/fapi/v1/userTrades", {"symbol":"BTCUSDT","orderId":order_id})
    return r.json() if r.status_code==200 else []

def summarize_binance_order(order_id:int)->Tuple[float,float,float]:
    fills=binance_user_trades(order_id); qty=quote=fee=0.0
    for f in fills:
        q=float(f.get("qty","0") or 0.0); p=float(f.get("price","0") or 0.0); c=float(f.get("commission","0") or 0.0)
        qty+=q; quote+=q*p; fee+=c
    avg=(quote/qty) if qty>0 else 0.0
    return qty, avg, fee

# ---------- PnL / ROI ----------
def compute_realized_pnl_krw(
    qty: float,
    ub_buy_avg: float, ub_sell_avg: float,
    ub_fee_in_krw: float, ub_fee_out_krw: float,
    bn_sell_avg: float, bn_buy_avg: float,
    bn_fee_in_usdt: float, bn_fee_out_usdt: float,
    usdkrw_exit: float, funding_usdt: float = 0.0
) -> float:
    krw_in  = ub_buy_avg  * qty + ub_fee_in_krw
    krw_out = ub_sell_avg * qty - ub_fee_out_krw
    upbit_realized = krw_out - krw_in
    usdt_pnl = (bn_sell_avg - bn_buy_avg) * qty
    usdt_realized = usdt_pnl - (bn_fee_in_usdt + bn_fee_out_usdt + funding_usdt)
    return upbit_realized + usdt_realized * usdkrw_exit

def compute_roi_pct(krw_spent: float, bn_notional_usdt: float, lev: int, usdkrw_entry: float, realized_krw: float) -> float:
    capital = krw_spent + (bn_notional_usdt/max(1,lev)) * usdkrw_entry
    return (realized_krw / capital * 100.0) if capital>0 else 0.0

# ---------- leg helpers ----------
def _aggregate_entry_info():
    entry_info["upbit_qty"]  = round(sum(l["upbit_qty"] for l in open_legs), 3)
    entry_info["binance_qty"]= round(sum(l["binance_qty"] for l in open_legs), 3)

def enter_leg(amount_krw: float, leverage:int, kimp:float, up:float, bp:float, fx:float, exit_kimp_abs:float)->Optional[Dict[str,Any]]:
    BIN_QTY_STEP = 0.001
    USDT_TOL = 5.0
    def _floor_step(x: float, step: float) -> float:
        return math.floor(float(x) / step) * step

    global next_leg_id

    pre_target_usdt = amount_krw / fx
    need_margin = pre_target_usdt / max(1, leverage)
    if get_binance_balance_real() + 1e-6 < need_margin:
        log(f"[레그 진입 중단] 증거금 부족 have<{need_margin:.2f}")
        return None

    ok_u, buy_uuid = upbit_order("buy", amount_krw, 0.0)
    if not ok_u or not buy_uuid:
        log("[레그 진입 실패] Upbit 매수 실패")
        return None

    funds_krw, ub_qty, ub_fee_krw, ub_avg = 0.0, 0.0, 0.0, 0.0
    for _ in range(10):
        time.sleep(0.35)
        funds_krw, ub_qty, ub_fee_krw, ub_avg = summarize_upbit_order(buy_uuid)
        if ub_qty > 0: break
    if ub_qty <= 0:
        log("[레그 진입 실패] Upbit 체결 수량 0")
        return None

    target_usdt = funds_krw / fx
    bp_now = get_binance_price()
    bn_target_qty = _floor_step(target_usdt / bp_now, BIN_QTY_STEP)
    if bn_target_qty <= 0:
        log("[레그 진입 실패] 바이낸스 산정 수량 0")
        return None

    ok_b, sell_id  = binance_order("sell", bp_now, bn_target_qty, reduce_only=False)
    if not ok_b or not sell_id:
        log("[레그 진입 실패] Binance 숏 실패 → 업비트 되돌림")
        upbit_order("sell", 0.0, ub_qty); return None

    bn_qty, bn_avg, bn_fee = summarize_binance_order(sell_id)
    if bn_qty <= 0:
        log("[레그 진입 실패] Binance 체결 수량 0 → 업비트 되돌림")
        upbit_order("sell", 0.0, ub_qty); return None

    # notional fine-tune
    for _ in range(8):
        bp_now = get_binance_price()
        cur_usdt = bn_qty * bp_now
        diff = target_usdt - cur_usdt
        if abs(diff) <= USDT_TOL: break
        adj_qty = _floor_step(abs(diff) / bp_now, BIN_QTY_STEP)
        if adj_qty <= 0: break
        if diff > 0:
            ok, oid = binance_order("sell", bp_now, adj_qty, reduce_only=False)
            if ok and oid:
                q, a, f = summarize_binance_order(oid)
                bn_qty += q; bn_fee += f
        else:
            ok, oid = binance_order("buy", bp_now, adj_qty, reduce_only=True)
            if ok and oid:
                q, a, f = summarize_binance_order(oid)
                bn_qty -= q; bn_fee += f
        time.sleep(0.25)

    leg_qty = min(round(ub_qty, 3), round(bn_qty, 3))
    if leg_qty <= 0:
        log("[레그 진입 실패] 최종 수량 0")
        upbit_order("sell", 0.0, ub_qty); return None

    leg = {
        "id": next_leg_id, "entered_at": time.time(),
        "entry_kimp": float(kimp), "exit_kimp": float(exit_kimp_abs),
        "upbit_qty": round(leg_qty,3), "binance_qty": round(leg_qty,3),
        "upbit_buy_uuid": buy_uuid, "upbit_sell_uuid": None,
        "binance_sell_id": sell_id, "binance_buy_id": None,
        "ub_avg": ub_avg, "ub_fee_krw": ub_fee_krw,
        "bn_avg": bn_avg, "bn_fee_usdt": bn_fee,
        "usdkrw_entry": fx, "lev": leverage,
        "krw_spent": funds_krw,
        "bn_notional_usdt": bn_qty * get_binance_price(),
        "realized_krw": None, "roi_pct": None
    }
    next_leg_id += 1
    log(f"[레그 진입] id={leg['id']} entry_kimp={kimp} exit_kimp={exit_kimp_abs}")
    return leg

def exit_leg(leg: Dict[str,Any], fx_exit: float)->bool:
    ok_u, sell_uuid = upbit_order("sell", 0.0, float(leg["upbit_qty"]))
    ok_b, buy_id    = binance_order("buy", 0.0, abs(float(leg["binance_qty"])), reduce_only=True)
    if not (ok_u and ok_b):
        log("[레그 청산 실패] 주문 실패"); return False

    leg["upbit_sell_uuid"] = sell_uuid; leg["binance_buy_id"] = buy_id

    buy_krw, qty_buy, buy_fee_krw, ub_buy_avg = summarize_upbit_order(leg["upbit_buy_uuid"])
    sell_krw, qty_sell, sell_fee_krw, ub_sell_avg = summarize_upbit_order(leg["upbit_sell_uuid"])
    qty_e, bn_sell_avg, fee_e = summarize_binance_order(leg["binance_sell_id"])
    qty_x, bn_buy_avg,  fee_x = summarize_binance_order(leg["binance_buy_id"])

    qty = min(float(leg["upbit_qty"]), float(leg["binance_qty"]), qty_buy, qty_sell, qty_e, qty_x)
    if qty <= 0:
        log("[레그 청산 경고] 유효 수량 0"); return False

    realized_krw = compute_realized_pnl_krw(
        qty=qty,
        ub_buy_avg=ub_buy_avg, ub_sell_avg=ub_sell_avg,
        ub_fee_in_krw=buy_fee_krw, ub_fee_out_krw=sell_fee_krw,
        bn_sell_avg=bn_sell_avg, bn_buy_avg=bn_buy_avg,
        bn_fee_in_usdt=fee_e, bn_fee_out_usdt=fee_x,
        usdkrw_exit=fx_exit, funding_usdt=0.0
    )

    global profit_krw, fees_upbit_krw_cum, fees_binance_usdt_cum, fees_binance_krw_cum
    profit_krw += realized_krw
    fees_upbit_krw_cum += (buy_fee_krw + sell_fee_krw)
    fees_binance_usdt_cum += (fee_e + fee_x)
    fees_binance_krw_cum  += (fee_e + fee_x) * fx_exit

    leg["realized_krw"] = realized_krw
    leg["roi_pct"] = compute_roi_pct(
        krw_spent=float(leg["krw_spent"]),
        bn_notional_usdt=float(leg["bn_notional_usdt"]),
        lev=int(leg["lev"]),
        usdkrw_entry=float(leg["usdkrw_entry"]),
        realized_krw=realized_krw
    )
    log(f"[레그 청산] id={leg['id']} PnL={realized_krw:+,.0f} KRW (ROI {leg['roi_pct']:.3f}%)")
    return True

def sweep_if_no_legs():
    with state_lock:
        if len(open_legs) > 0:
            return
    _, btc_bal = get_upbit_balance_real()
    if btc_bal > 1e-6:
        ok, _ = upbit_order("sell", 0.0, btc_bal)
        if ok: log(f"[스윕] 업비트 잔량 {btc_bal:.6f} BTC 매도")
    pos = get_binance_position_qty()
    if abs(pos) > 1e-6:
        side = "buy" if pos < 0 else "sell"
        ok, _ = binance_order(side, 0.0, abs(pos), reduce_only=True)
        if ok: log(f"[스윕] 바이낸스 잔량 {pos:+.3f} BTC reduceOnly 청산")

# ---------- strategy loop ----------
def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global running, position_state, trade_count, last_error, last_cfg
    last_cfg = cfg.copy()

    sync_binance_time(); set_binance_leverage("BTCUSDT", int(float(cfg["leverage"])))

    mode          = cfg.get("mode","stack")         # "stack" | "flip"
    allow_multi   = bool(cfg.get("allow_multi_entry", True))
    max_legs      = int(cfg.get("max_legs", 3))
    min_gap_bp    = float(cfg.get("min_reenter_gap_bp", 0.15))
    exit_on_sign  = bool(cfg.get("exit_on_sign_change", True))
    exit_on_move  = float(cfg.get("exit_on_move_bp", 0.0))
    target_kimp   = float(cfg["target_kimp"])
    exit_kimp_abs = float(cfg["exit_kimp"])
    tol           = float(cfg["tolerance"])
    amount_krw    = float(cfg["amount_krw"])
    leverage      = int(float(cfg["leverage"]))

    # flip presets
    pos_entry = float(cfg.get("pos_entry_kimp", 0.3))
    pos_exit  = float(cfg.get("pos_exit_kimp", 0.6))
    neg_entry = float(cfg.get("neg_entry_kimp",-0.6))
    neg_exit  = float(cfg.get("neg_exit_kimp",-0.9))

    def _auto_exit_list(kimp_now: float) -> List[int]:
        to_close: List[int] = []
        with state_lock:
            for leg in open_legs:
                crossed = (kimp_now >= leg["exit_kimp"]) if leg["exit_kimp"] >= leg["entry_kimp"] else (kimp_now <= leg["exit_kimp"])
                signchg = exit_on_sign and ((leg["entry_kimp"] >= 0 and kimp_now < 0) or (leg["entry_kimp"] < 0 and kimp_now >= 0))
                moved   = (exit_on_move > 0 and abs(kimp_now - leg["entry_kimp"]) >= exit_on_move)
                if crossed or signchg or moved: to_close.append(leg["id"])
        return to_close

    while running:
        try:
            metrics["loops"] += 1
            kimp, up, bp, fx = calc_kimp()

            if mode == "flip":
                # 1) 플립 모드 진입/전환 로직
                with state_lock:
                    have_leg = len(open_legs) > 0

                want_pos = abs(kimp - pos_entry) <= tol
                want_neg = abs(kimp - neg_entry) <= tol

                if not have_leg and (want_pos or want_neg):
                    chosen_exit = pos_exit if want_pos else neg_exit
                    leg = enter_leg(amount_krw, leverage, kimp, up, bp, fx, chosen_exit)
                    if leg:
                        with state_lock:
                            open_legs[:] = [leg]
                            _aggregate_entry_info()
                            position_state = "entered"
                            log(f"[플립 진입] {('POS' if want_pos else 'NEG')} @ {kimp}%")
                elif have_leg:
                    need_flip_to = None
                    with state_lock:
                        cur_sign = 1 if open_legs[0]["entry_kimp"] >= 0 else -1
                    if cur_sign > 0 and want_neg:      need_flip_to = "neg"
                    elif cur_sign < 0 and want_pos:    need_flip_to = "pos"

                    if need_flip_to:
                        ids=[]
                        with state_lock: ids = [l["id"] for l in open_legs]
                        for leg_id in ids:
                            with state_lock:
                                leg = next((l for l in open_legs if l["id"]==leg_id), None)
                            if leg and exit_leg(leg, fx):
                                with state_lock:
                                    open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
                                    closed_legs_recent.append(leg)
                                    if len(closed_legs_recent)>5: closed_legs_recent.pop(0)
                                    _aggregate_entry_info()
                                    trade_count += 1
                        sweep_if_no_legs()

                        chosen_exit = pos_exit if need_flip_to=="pos" else neg_exit
                        leg = enter_leg(amount_krw, leverage, kimp, up, bp, fx, chosen_exit)
                        if leg:
                            with state_lock:
                                open_legs[:] = [leg]
                                _aggregate_entry_info()
                                position_state = "entered"
                                log(f"[플립 전환] → {need_flip_to.upper()} @ {kimp}%")

                # 2) 자동 청산 조건
                for leg_id in _auto_exit_list(kimp):
                    with state_lock:
                        leg = next((l for l in open_legs if l["id"]==leg_id), None)
                    if leg and exit_leg(leg, get_usdkrw()):
                        with state_lock:
                            open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
                            closed_legs_recent.append(leg)
                            if len(closed_legs_recent)>5: closed_legs_recent.pop(0)
                            _aggregate_entry_info()
                            trade_count += 1
                            position_state = "neutral" if len(open_legs)==0 else "entered"
                        sweep_if_no_legs()

            else:
                # ---- 스택 모드 ----
                with state_lock:
                    if allow_multi and abs(kimp - target_kimp) <= tol and len(open_legs) < max_legs:
                        ok_gap = all(abs(kimp - leg["entry_kimp"]) >= min_gap_bp for leg in open_legs)
                        if ok_gap:
                            leg = enter_leg(amount_krw, leverage, kimp, up, bp, fx, exit_kimp_abs)
                            if leg:
                                open_legs.append(leg)
                                _aggregate_entry_info()
                                position_state = "entered"
                                log(f"[레그 진입] id={leg['id']} qty U:{leg['upbit_qty']} / B:{leg['binance_qty']} @ kimp {kimp}%")

                for leg_id in _auto_exit_list(kimp):
                    with state_lock:
                        leg = next((l for l in open_legs if l["id"]==leg_id), None)
                    if leg and exit_leg(leg, get_usdkrw()):
                        with state_lock:
                            open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
                            closed_legs_recent.append(leg)
                            if len(closed_legs_recent) > 5: closed_legs_recent.pop(0)
                            _aggregate_entry_info()
                            trade_count += 1
                            position_state = "neutral" if len(open_legs)==0 else "entered"
                        sweep_if_no_legs()

            time.sleep(0.45)

        except Exception as e:
            metrics["api_errors"] += 1; last_error = str(e); log(f"[전략 오류] {e}")
            time.sleep(0.7)

# ---------- routes ----------
@app.route("/")
def index(): return render_template("index.html")

@app.route("/current_kimp")
def current_kimp():
    k, up, bp, fx = calc_kimp()
    return jsonify({"kimp":k,"upbit_price":up,"binance_price":bp,"usdkrw":fx})

@app.route("/balance")
def balance():
    krw, btc = get_upbit_balance_real(); usdt = get_binance_balance_real()
    return jsonify({"real":{"krw":round(krw,0),"btc_upbit":round(btc,6),"usdt":round(usdt,3)}})

@app.route("/status")
def status():
    fx_now = get_usdkrw(); fees_binance_krw_now = fees_binance_usdt_cum * fx_now
    with state_lock:
        legs = list(open_legs)
        last_closed = closed_legs_recent[-1] if closed_legs_recent else None
        aggregate_live = len(legs)>0
    return jsonify({
        "running": running,
        "position_state": "entered" if aggregate_live else "neutral",
        "trade_count": trade_count,
        "logs": logs[-350:],
        "entry_info": entry_info,
        "last_error": last_error,
        "binance_testnet": BINANCE_TESTNET, "binance_base": BASE_FAPI,
        "pnl": {
            "profit_krw_cum": round(profit_krw, 0),
            "fees_upbit_krw_cum": round(fees_upbit_krw_cum, 0),
            "fees_binance_usdt_cum": round(fees_binance_usdt_cum, 3),
            "fees_binance_krw_cum": round(fees_binance_krw_cum, 0),
            "fees_binance_krw_now": round(fees_binance_krw_now, 0),
        },
        "open_legs": legs,
        "last_closed_leg": last_closed,
        "cfg": last_cfg,
    })

@app.route("/start", methods=["POST"])
def start():
    global running, last_error, last_cfg
    if not running:
        keys = load_api_keys()
        if not keys.get("binance_key") or not keys.get("binance_secret"):
            last_error="binance_api_key_missing"; log("[전략 시작 실패] BINANCE 키 누락")
            return jsonify({"status":"error","error":last_error}), 400
        cfg_in = request.json or {}
        cfg = {
            # 공통
            "mode":            cfg_in.get("mode","stack"),
            "target_kimp":     float(cfg_in.get("target_kimp", 0.0)),
            "exit_kimp":       float(cfg_in.get("exit_kimp", 0.3)),
            "tolerance":       float(cfg_in.get("tolerance", 0.1)),
            "amount_krw":      float(cfg_in.get("amount_krw", 1_000_000)),
            "leverage":        int(float(cfg_in.get("leverage", 3))),
            "exit_on_sign_change": bool(cfg_in.get("exit_on_sign_change", True)),
            "exit_on_move_bp": float(cfg_in.get("exit_on_move_bp", 0.0)),
            # 스택 옵션
            "allow_multi_entry": bool(cfg_in.get("allow_multi_entry", True)),
            "max_legs": int(cfg_in.get("max_legs", 3)),
            "min_reenter_gap_bp": float(cfg_in.get("min_reenter_gap_bp", 0.15)),
            # 플립 프리셋
            "pos_entry_kimp": float(cfg_in.get("pos_entry_kimp", 0.3)),
            "pos_exit_kimp":  float(cfg_in.get("pos_exit_kimp", 0.6)),
            "neg_entry_kimp": float(cfg_in.get("neg_entry_kimp",-0.6)),
            "neg_exit_kimp":  float(cfg_in.get("neg_exit_kimp",-0.9)),
        }
        # 플립 모드는 단일 레그 강제
        if cfg["mode"] == "flip":
            cfg["allow_multi_entry"] = False
            cfg["max_legs"] = 1
        running=True
        last_cfg = cfg.copy()
        threading.Thread(target=run_strategy_thread, args=(cfg,), daemon=True).start()
        log(f"[전략 시작] cfg={cfg} testnet={BINANCE_TESTNET} base={BASE_FAPI}")
    return jsonify({"status":"started"})

@app.route("/stop", methods=["POST"])
def stop():
    global running
    running=False; log("[전략 중지]"); return jsonify({"status":"stopped"})

@app.route("/force_exit", methods=["POST"])
def force_exit():
    closed = 0
    with state_lock:
        ids = [l["id"] for l in open_legs]
    for leg_id in ids:
        with state_lock:
            leg = next((l for l in open_legs if l["id"]==leg_id), None)
        if not leg: continue
        if exit_leg(leg, get_usdkrw()):
            with state_lock:
                open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
                closed_legs_recent.append(leg)
                if len(closed_legs_recent)>5: closed_legs_recent.pop(0)
                _aggregate_entry_info()
                globals()["trade_count"] += 1
    with state_lock:
        globals()["position_state"] = "neutral" if len(open_legs)==0 else "entered"
    sweep_if_no_legs()
    return jsonify({"ok": True, "closed_count": closed})

# 수동 즉시 진입(모드별 반영)
@app.route("/manual_enter", methods=["POST"])
def manual_enter():
    data = request.json or {}
    amt = float(data.get("amount_krw", 0))
    lev = int(float(data.get("leverage", 3)))
    exit_k = data.get("exit_kimp", None)

    kimp, up, bp, fx = calc_kimp()
    if exit_k is None or str(exit_k)=="":
        exit_k = (kimp + 0.3) if kimp >= 0 else (kimp - 0.3)

    leg = enter_leg(amt, lev, kimp, up, bp, fx, float(exit_k))
    if not leg: return jsonify({"ok": False, "error":"enter failed"}), 409

    with state_lock:
        if (last_cfg or {}).get("mode","stack") == "stack":
            open_legs.append(leg)
        else:
            open_legs[:] = [leg]
        _aggregate_entry_info()
        globals()["position_state"] = "entered"

    return jsonify({"ok": True, "leg": leg})

# 즉시 플립: 현재 레그 모두 청산 → (기본) 바로 재진입 / (옵션) 청산만
@app.route("/flip_now", methods=["POST"])
def flip_now():
    cfg = last_cfg or {}
    body = request.json or {}
    to = body.get("to", None)                 # "pos" | "neg" | None(자동결정)
    reenter_now = bool(body.get("reenter_now", True))  # 기본: 즉시 전환

    pos_entry = float(cfg.get("pos_entry_kimp", 0.3))
    pos_exit  = float(cfg.get("pos_exit_kimp", 0.6))
    neg_entry = float(cfg.get("neg_entry_kimp",-0.6))
    neg_exit  = float(cfg.get("neg_exit_kimp",-0.9))
    amt = float(cfg.get("amount_krw", 1_000_000))
    lev = int(float(cfg.get("leverage", 3)))

    # 1) 모두 청산
    closed = 0
    with state_lock: ids = [l["id"] for l in open_legs]
    for leg_id in ids:
        with state_lock:
            leg = next((l for l in open_legs if l["id"]==leg_id), None)
        if leg and exit_leg(leg, get_usdkrw()):
            with state_lock:
                open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
                closed_legs_recent.append(leg)
                if len(closed_legs_recent)>5: closed_legs_recent.pop(0)
                _aggregate_entry_info()
                globals()["trade_count"] += 1
                globals()["position_state"] = "neutral"
            closed += 1
    sweep_if_no_legs()

    # 2) 대상 프리셋 결정
    kimp, up, bp, fx = calc_kimp()
    if to not in ("pos","neg"):
        # 현재 값에서 어느 엔트리와 더 가까운지로 자동결정
        to = "pos" if abs(kimp - pos_entry) <= abs(kimp - neg_entry) else "neg"
    chosen_exit = pos_exit if to=="pos" else neg_exit

    # 3) 즉시 재진입이 아니면 여기서 종료(프리셋 모드 유지)
    if not reenter_now:
        log(f"[플립] 전부 청산 완료({closed}) → {to.upper()} 프리셋 대기")
        with state_lock:
            last_cfg.setdefault("mode","flip")
            last_cfg["allow_multi_entry"] = False
            last_cfg["max_legs"] = 1
        return jsonify({"ok": True, "closed": closed, "armed_to": to, "reenter_now": False})

    # 4) 바로 진입
    leg = enter_leg(amt, lev, kimp, up, bp, fx, chosen_exit)
    if not leg: return jsonify({"ok": False, "closed": closed, "error":"enter failed"}), 409
    with state_lock:
        open_legs[:] = [leg]
        _aggregate_entry_info()
        globals()["position_state"] = "entered"
        last_cfg.setdefault("mode","flip")
        last_cfg["allow_multi_entry"] = False
        last_cfg["max_legs"] = 1
    log(f"[플립 전환] 즉시 → {to.upper()} @ {kimp}%")
    return jsonify({"ok": True, "closed": closed, "leg": leg, "to": to, "reenter_now": True})

@app.route("/close_leg", methods=["POST"])
def close_leg():
    data = request.json or {}
    leg_id = int(data.get("id", 0))
    if not leg_id: return jsonify({"ok": False, "error":"missing id"}), 400
    with state_lock:
        leg = next((l for l in open_legs if l["id"]==leg_id), None)
    if not leg: return jsonify({"ok": False, "error":"not found"}), 404
    ok = exit_leg(leg, get_usdkrw())
    if ok:
        with state_lock:
            open_legs[:] = [l for l in open_legs if l["id"]!=leg_id]
            closed_legs_recent.append(leg)
            if len(closed_legs_recent)>5: closed_legs_recent.pop(0)
            _aggregate_entry_info()
            globals()["trade_count"] += 1
            globals()["position_state"] = "neutral" if len(open_legs)==0 else "entered"
        sweep_if_no_legs()
        return jsonify({"ok": True})
    return jsonify({"ok": False}), 409

@app.route("/health")
def health(): return jsonify({"thread_alive": bool(running)})
@app.route("/metrics")
def metrics_route(): return jsonify(metrics)

if __name__ == "__main__":
    sync_binance_time()
    app.run(host="0.0.0.0", port=5000, debug=False)
