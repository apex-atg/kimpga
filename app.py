# app.py — Upbit Spot LONG + Binance Futures SHORT
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
    retry = Retry(total=5, backoff_factor=0.5,
                  status_forcelist=(408,429,500,502,503,504),
                  allowed_methods=frozenset(["GET","POST"]),
                  raise_on_status=False)
    ad = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    s.mount("https://", ad); s.mount("http://", ad)
    return s

HTTP = _make_session()
TIMEOUT = (3, 7)

# ---------------- Fees/rounding ----------------
UPBIT_TAKER_FEE   = float(os.getenv("UPBIT_TAKER_FEE",  "0.0005"))  # 0.05%
UPBIT_OVERBUY_BP  = float(os.getenv("UPBIT_OVERBUY_BP", "0.02"))    # +0.02% 버퍼
def floor_step(x: float, step: float = 0.001) -> float:
    return math.floor(float(x)/step)*step

# ---------------- Global State ----------------
running = False
position_state = "neutral"
trade_count = 0

profit_krw = 0.0
fees_upbit_krw_cum = 0.0
fees_binance_usdt_cum = 0.0
fees_binance_krw_cum = 0.0

logs: List[str] = []
last_error: Optional[str] = None
metrics = {"loops":0, "orders_binance":0, "orders_upbit":0, "api_errors":0}

entry_info: Dict[str, float] = {"upbit_qty":0.0, "binance_qty":0.0}

# 스티키 포지션 표시용
current_position: Optional[Dict[str,Any]] = None
last_position: Optional[Dict[str,Any]] = None
entry_kimp_value: Optional[float] = None

# 한 사이클 기록 (PnL 계산용)
current_cycle: Dict[str, Any] = {
    "upbit_buy_uuid": None, "upbit_sell_uuid": None,
    "upbit_buy_krw": 0.0, "upbit_sell_krw": 0.0, "upbit_fee_krw": 0.0,
    "binance_sell_id": None, "binance_buy_id": None,
    "binance_entry_qty": 0.0, "binance_entry_avg": 0.0,
    "binance_exit_qty": 0.0, "binance_exit_avg": 0.0,
    "binance_fee_usdt": 0.0,
}

def reset_cycle():
    for k in list(current_cycle.keys()):
        current_cycle[k] = 0.0 if isinstance(current_cycle[k], (int,float)) else None

def log(msg: str):
    ts = time.strftime("[%H:%M:%S]"); logs.append(f"{ts} {msg}")
    if len(logs) > 500: del logs[:200]

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

def summarize_upbit_order(uuid_str:str)->Tuple[float,float,float]:
    d=upbit_order_detail(uuid_str)
    fee=float(d.get("paid_fee","0") or 0.0); funds=vol=0.0
    for t in d.get("trades",[]): 
        vol+=float(t.get("volume","0") or 0.0); funds+=float(t.get("funds","0") or 0.0)
    return funds, vol, fee

def get_binance_position_qty()->float:
    r=_bn_signed_get("/fapi/v2/positionRisk")
    if r.status_code==200:
        for p in r.json():
            if p.get("symbol")=="BTCUSDT":
                try: return float(p.get("positionAmt",0.0))  # 숏<0, 롱>0
                except Exception: return 0.0
    return 0.0

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

def summarize_binance_order(order_id:int)->Tuple[float,float,float]:
    fills=binance_user_trades(order_id); qty=quote=fee=0.0
    for f in fills:
        q=float(f.get("qty","0") or 0.0); p=float(f.get("price","0") or 0.0); c=float(f.get("commission","0") or 0.0)
        qty+=q; quote+=q*p; fee+=c
    avg=(quote/qty) if qty>0 else 0.0
    return qty, avg, fee

# ---------------- Exit helpers ----------------
def full_exit_with_retries(max_retries:int=6, retry_delay:float=1.0)->Tuple[bool,Optional[str],Optional[int]]:
    """업비트 잔량=0 & 바이낸스 포지션=0 될 때까지 반복"""
    up_uuid=None; bn_oid=None
    for i in range(1, max_retries+1):
        up_btc = round(get_upbit_balance_real()[1], 6)
        pos = get_binance_position_qty()  # 숏<0
        need_up = up_btc > 0.00005
        need_bn = pos < -1e-6
        log(f"[청산 체크#{i}] upbit={up_btc:.6f}BTC, binance={pos:.6f}")
        ok_u=True; ok_b=True
        if need_up: ok_u, up_uuid = upbit_order("sell", 0.0, up_btc)
        if need_bn: ok_b, bn_oid  = binance_order("buy", 0.0, abs(pos), reduce_only=True)
        time.sleep(retry_delay)
        new_up = round(get_upbit_balance_real()[1], 6)
        new_pos = get_binance_position_qty()
        closed = (new_up<=0.00005) and (-1e-6<=new_pos<=1e-6)
        log(f"[청산 재확인#{i}] upbit={new_up:.6f}, binance_pos={new_pos:.6f} → {'OK' if closed else 'RETRY'}")
        if ok_u and ok_b and closed: return True, up_uuid, bn_oid
    return False, up_uuid, bn_oid

def compute_cycle_pnl_and_log(fx_for_exit: float) -> float:
    global fees_upbit_krw_cum, fees_binance_usdt_cum, fees_binance_krw_cum
    # Upbit
    if current_cycle["upbit_buy_uuid"]:
        krw, qty, fee = summarize_upbit_order(current_cycle["upbit_buy_uuid"])
        current_cycle["upbit_buy_krw"] = krw; current_cycle["upbit_fee_krw"] += fee; entry_info["upbit_qty"]=qty
    if current_cycle["upbit_sell_uuid"]:
        krw, _, fee = summarize_upbit_order(current_cycle["upbit_sell_uuid"])
        current_cycle["upbit_sell_krw"] = krw; current_cycle["upbit_fee_krw"] += fee
    upbit_pnl = current_cycle["upbit_sell_krw"] - current_cycle["upbit_buy_krw"] - current_cycle["upbit_fee_krw"]
    # Binance
    if current_cycle["binance_sell_id"]:
        q,a,f = summarize_binance_order(current_cycle["binance_sell_id"])
        current_cycle["binance_entry_qty"]=q; current_cycle["binance_entry_avg"]=a; current_cycle["binance_fee_usdt"]+=f; entry_info["binance_qty"]=q
    if current_cycle["binance_buy_id"]:
        q,a,f = summarize_binance_order(current_cycle["binance_buy_id"])
        current_cycle["binance_exit_qty"]=q; current_cycle["binance_exit_avg"]=a; current_cycle["binance_fee_usdt"]+=f
    qty_close=min(current_cycle["binance_entry_qty"], current_cycle["binance_exit_qty"])
    fut_pnl_usdt=(current_cycle["binance_entry_avg"]-current_cycle["binance_exit_avg"])*qty_close - current_cycle["binance_fee_usdt"]
    total_krw = upbit_pnl + fut_pnl_usdt * fx_for_exit
    fees_upbit_krw_cum += current_cycle["upbit_fee_krw"]
    fees_binance_usdt_cum += current_cycle["binance_fee_usdt"]
    fees_binance_krw_cum  += current_cycle["binance_fee_usdt"] * fx_for_exit
    log(f"[실현손익] {total_krw:+,.0f} KRW (Upbit {upbit_pnl:+,.0f} + Bin {fut_pnl_usdt:+.3f}USDT)")
    return total_krw

# ---------------- Strategy loop ----------------
def run_strategy_thread(cfg: Dict[str, Any]) -> None:
    global running, position_state, trade_count, entry_kimp_value, profit_krw
    global current_position, last_position, last_error

    # ★ 핵심: 금액 입력은 BTC 수량
    target_kimp = float(cfg["target_kimp"])
    exit_kimp   = float(cfg["exit_kimp"])
    tolerance   = float(cfg["tolerance"])
    amount_btc  = max(0.001, float(cfg["amount_btc"]))      # ← BTC
    leverage    = int(float(cfg["leverage"]))
    exit_on_sign_change = bool(cfg.get("exit_on_sign_change", True))
    exit_on_move_bp     = float(cfg.get("exit_on_move_bp", 0.0))

    sync_binance_time()
    set_binance_leverage("BTCUSDT", leverage)

    while running:
        try:
            metrics["loops"] += 1
            kimp, up, bp, fx = calc_kimp()

            # ------- Entry -------
            if position_state == "neutral" and abs(kimp - target_kimp) <= tolerance:
                desired_btc = floor_step(amount_btc, 0.001)
                if desired_btc <= 0: time.sleep(0.4); continue

                # 업비트는 price 주문이므로 총 지불 KRW 산출(수수료/버퍼 포함)
                gross_krw = desired_btc * up / max(1e-9, (1.0 - UPBIT_TAKER_FEE))
                gross_krw *= (1.0 + UPBIT_OVERBUY_BP/100.0)
                gross_krw_int = int(math.ceil(gross_krw))

                reset_cycle()
                ok_u, u_uuid = upbit_order("buy", gross_krw_int, desired_btc)
                if not ok_u or not u_uuid:
                    log("[진입 실패] 업비트 매수 실패"); time.sleep(0.4); continue
                current_cycle["upbit_buy_uuid"] = u_uuid

                # 업비트 체결 수량 확인
                filled_qty = 0.0
                for _ in range(6):
                    time.sleep(0.2)
                    _, vol, _fee = summarize_upbit_order(u_uuid)
                    if vol > 0: filled_qty = vol; break
                if filled_qty <= 0: filled_qty = desired_btc

                # 바이낸스 숏 수량은 업비트 체결 기준(안전하게 내림)
                binance_qty = floor_step(filled_qty, 0.001)
                if binance_qty <= 0:
                    upbit_order("sell", 0.0, filled_qty)
                    reset_cycle(); log("[진입 취소] 업비트 체결 0"); continue

                # 증거금 체크
                need_usdt = (binance_qty * bp) / max(1, leverage)
                have_usdt = get_binance_balance_real()
                if have_usdt + 1e-6 < need_usdt:
                    log(f"[진입 중단] USDT 부족 have≈{have_usdt:.2f}, need≈{need_usdt:.2f}")
                    upbit_order("sell", 0.0, filled_qty)
                    reset_cycle(); time.sleep(0.4); continue

                ok_b, b_id = binance_order("sell", bp, binance_qty)
                if not ok_b or not b_id:
                    log("[진입 실패] 바이낸스 숏 실패 → 업비트 되돌림")
                    upbit_order("sell", 0.0, filled_qty)
                    reset_cycle(); time.sleep(0.4); continue
                current_cycle["binance_sell_id"] = b_id

                entry_info.update({"upbit_qty":round(filled_qty,3),"binance_qty":round(binance_qty,3)})
                position_state = "entered"
                entry_kimp_value = float(kimp)
                current_position = {
                    "live": True, "side":"LS",
                    "upbit_qty": filled_qty, "binance_qty": binance_qty,
                    "entry_kimp": entry_kimp_value, "entered_at": time.time()
                }
                log(f"[진입 성공] ~{filled_qty:.3f} BTC / 김프 {kimp}%")
                time.sleep(0.4)

            # ------- Exit -------
            if position_state == "entered":
                crossed = (kimp >= exit_kimp) if (entry_kimp_value is None or exit_kimp >= entry_kimp_value) else (kimp <= exit_kimp)
                signchg = exit_on_sign_change and (entry_kimp_value is not None) and ((entry_kimp_value>=0 and kimp<0) or (entry_kimp_value<0 and kimp>=0))
                moved   = (exit_on_move_bp>0 and entry_kimp_value is not None and abs(kimp-entry_kimp_value)>=exit_on_move_bp)

                if crossed or signchg or moved:
                    why="exit_kimp" if crossed else ("sign_change" if signchg else "move_trigger")
                    log(f"[청산 트리거:{why}] entry={entry_kimp_value}, now={kimp}")

                    done, u_uuid, b_id = full_exit_with_retries(max_retries=6, retry_delay=1.0)
                    if done:
                        if u_uuid: current_cycle["upbit_sell_uuid"]=u_uuid
                        if b_id:   current_cycle["binance_buy_id"]=b_id
                        pnl = compute_cycle_pnl_and_log(get_usdkrw())
                        profit_krw += pnl
                        trade_count += 1
                        position_state = "neutral"
                        entry_info.update({"upbit_qty":0.0,"binance_qty":0.0})
                        if current_position:
                            current_position.update({"live":False,"closed_at":time.time(),"close_kimp":kimp})
                            last_position=current_position.copy()
                        else:
                            last_position=None
                        entry_kimp_value=None; current_position=None; reset_cycle()
                        log("[청산 완료] → 중립")
                    else:
                        log("[청산 미완료] 다음 루프 재시도")

        except Exception as e:
            metrics["api_errors"] += 1; last_error = str(e); log(f"[전략 오류] {e}")

        time.sleep(0.5)

# ---------------- Routes ----------------
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
    display_position = current_position if current_position is not None else last_position
    display_is_live = bool(display_position and display_position.get("live", False))
    return jsonify({
        "running": running, "position_state": position_state, "trade_count": trade_count,
        "logs": logs[-300:], "entry_info": entry_info, "last_error": last_error,
        "binance_testnet": BINANCE_TESTNET, "binance_base": BASE_FAPI,
        "entry_kimp_value": entry_kimp_value,
        "pnl": {
            "profit_krw_cum": round(profit_krw, 0),
            "fees_upbit_krw_cum": round(fees_upbit_krw_cum, 0),
            "fees_binance_usdt_cum": round(fees_binance_usdt_cum, 3),
            "fees_binance_krw_cum": round(fees_binance_krw_cum, 0),
            "fees_binance_krw_now": round(fees_binance_krw_now, 0),
        },
        "display_position": display_position,
        "display_is_live": display_is_live,
    })

@app.route("/start", methods=["POST"])
def start():
    global running, last_error
    if not running:
        keys = load_api_keys()
        if not keys.get("binance_key") or not keys.get("binance_secret"):
            last_error="binance_api_key_missing"; log("[전략 시작 실패] BINANCE 키 누락")
            return jsonify({"status":"error","error":last_error}), 400
        cfg_in = request.json or {}
        try:
            cfg = {
                "target_kimp": float(cfg_in.get("target_kimp", 0.0)),
                "exit_kimp":   float(cfg_in.get("exit_kimp", 0.3)),
                "tolerance":   float(cfg_in.get("tolerance", 0.1)),
                "amount_btc":  float(cfg_in.get("amount_btc", 0.01)),   # ★ BTC
                "leverage":    int(float(cfg_in.get("leverage", 3))),
                # 옵션
                "exit_on_sign_change": bool(cfg_in.get("exit_on_sign_change", True)),
                "exit_on_move_bp":     float(cfg_in.get("exit_on_move_bp", 0.0)),
            }
        except Exception as e:
            return jsonify({"status":"error","error":f"bad_config: {e}"}), 400
        running=True
        threading.Thread(target=run_strategy_thread, args=(cfg,), daemon=True).start()
        log(f"[전략 시작] cfg={cfg} testnet={BINANCE_TESTNET} base={BASE_FAPI}")
    return jsonify({"status":"started"})

@app.route("/stop", methods=["POST"])
def stop():
    global running
    running=False; log("[전략 중지]"); return jsonify({"status":"stopped"})

@app.route("/force_exit", methods=["POST"])
def force_exit():
    # 즉시 전체 청산(업비트 잔량 0 & 바이낸스 포지션 0 될 때까지 재시도)
    done, up_uuid, bn_oid = full_exit_with_retries(max_retries=6, retry_delay=1.0)
    if done:
        if up_uuid: current_cycle["upbit_sell_uuid"]=up_uuid
        if bn_oid:  current_cycle["binance_buy_id"]=bn_oid
        pnl = compute_cycle_pnl_and_log(get_usdkrw())
        globals()["profit_krw"] += pnl
        globals()["position_state"] = "neutral"
        entry_info.update({"upbit_qty":0.0,"binance_qty":0.0})
        globals()["last_position"] = (current_position or {}) or None
        globals()["current_position"] = None
        globals()["entry_kimp_value"] = None
        reset_cycle()
        log("[수동 청산 완료] → 중립")
        return jsonify({"ok": True})
    return jsonify({"ok": False}), 409

@app.route("/health")
def health(): return jsonify({"thread_alive": bool(running)})

@app.route("/metrics")
def metrics_route(): return jsonify(metrics)

if __name__ == "__main__":
    sync_binance_time()
    app.run(host="0.0.0.0", port=5000, debug=False)
