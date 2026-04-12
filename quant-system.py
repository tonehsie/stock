import streamlit as st
import requests
import pandas as pd
import numpy as np
import datetime
from io import StringIO
import time
import re
import concurrent.futures
import urllib.request
import ssl
import urllib3

# 📌 關閉所有憑證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定網頁標題與佈局
st.set_page_config(page_title="台股全息量化系統 (V24.0 完美數學校正版)", layout="wide")

# 內建最新 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0xMCAyMDoyMDo0NiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.7s3-IrkfdiUyTvGiZQGESBUBAPHQTnd4pwYcn8_J-CY"

# 📌 注入全局 CSS
st.markdown("""
<style>
table.dataframe th { text-align: center !important; }
table.radar-table td:last-child { text-align: left !important; }
</style>
""", unsafe_allow_html=True)

st.title("🤖 交易員實戰手冊：全息量化擷取系統")
st.markdown("✅ **V24.0 鐵桿鎖碼雷達 (完美數學校正)** | ✅ **低價反分身+智能門檻** | ✅ **鉅額(3日)+排版滿血**")

# UI 輸入區
col1, col2 = st.columns([1, 1])
with col1:
    stock_id = st.text_input("個股代號", value="1785")
with col2:
    dead_chip_input = st.text_input("死籌碼 %", placeholder="備註：以董監事持股比例為主，可自行輸入（包含大股東）。", help="備註：以董監事持股比例為主，可自行輸入（包含大股東）。")

st.write("")
run_btn = st.button("🚀 啟動引擎：擷取全息資料並產生 Prompt", use_container_width=True)

st.divider()

# ==========================================
# 工具函式與多重死籌碼引擎
# ==========================================

def get_stock_name(target_id):
    try:
        res = requests.get(f"https://tw.stock.yahoo.com/quote/{target_id}.TW", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        match = re.search(r'<title>(.*?)\s*\(', res.text)
        if match: return match.group(1).strip()
    except: pass
    return ""

def safe_get_fubon(url):
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        if hasattr(ssl, 'OP_LEGACY_SERVER_CONNECT'):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            return response.read().decode('big5', errors='ignore')
    except Exception as e:
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, verify=False)
            res.encoding = 'big5'
            return res.text
        except:
            return ""

def fetch_fm(dataset, start_date, end_date=None, specific_id=True, target_id=None):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": dataset, "start_date": start_date}
    if specific_id: params["data_id"] = stock_id
    elif target_id: params["data_id"] = target_id
    if end_date: params["end_date"] = end_date
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=15).json()
        return pd.DataFrame(res.get("data", []))
    except: return pd.DataFrame()

def format_to_gas(df, title):
    header = f"▼▼▼ {title} ▼▼▼, \n"
    if df is None or df.empty: return header + "此區塊查無最新數據或無發行紀錄, \n"
    csv_str = df.to_csv(index=False)
    lines = [line.replace('"', '') + ", " for line in csv_str.strip().split('\n')]
    return header + "\n".join(lines) + "\n"

def format_pledge_to_gas(df_summary, df_detail):
    if df_summary is None or df_summary.empty: return "▼▼▼ 18. 董監大股東質設 ▼▼▼, \n此區塊查無最新數據或無發行紀錄, \n"
    return format_to_gas(df_summary, "18. 董監大股東質設 (餘額與斷頭預警)") + format_to_gas(df_detail, "董監大股東質設 (異動明細)")

def scrape_director_holding(target_id):
    headers = {"User-Agent": "Mozilla/5.0"}
    debug_log = []
    dynamic_dict = {}
    static_val = 0.0
    chip_engine = "失敗"

    try:
        url_good = f"https://goodinfo.tw/tw/StockDirectorSharehold.asp?STOCK_ID={target_id}"
        headers_good = headers.copy()
        headers_good["Referer"] = f"https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID={target_id}"
        headers_good["Cookie"] = "CLIENT_KEY=20260411;" 
        res = requests.get(url_good, headers=headers_good, timeout=8)
        
        if res.status_code == 200:
            res.encoding = 'utf-8'
            dfs = pd.read_html(StringIO(res.text))
            for df in dfs:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = ['_'.join(str(c) for c in col if 'Unnamed' not in str(c)).strip('_') for col in df.columns.values]
                else:
                    df.columns = df.columns.astype(str)
                
                target_col = next((c for c in df.columns if '全體董監持股' in str(c) and '持股(%)' in str(c).replace(' ', '')), None)
                month_col = next((c for c in df.columns if '月別' in str(c)), None)
                
                if target_col and month_col:
                    latest_val = 0.0
                    for _, row in df.iterrows():
                        m_str = str(row[month_col]).replace('/', '-').strip()
                        v_str = str(row[target_col]).replace(',', '').strip()
                        
                        if re.match(r'^\d{4}-\d{2}$', m_str) and v_str not in ['-', '', 'nan']:
                            try:
                                val = float(v_str)
                                if 0 < val < 100.0:
                                    dynamic_dict[m_str] = val
                                    if latest_val == 0.0: latest_val = val
                            except: pass
                    
                    if dynamic_dict:
                        return dynamic_dict, latest_val, "Goodinfo", debug_log
    except Exception as e:
        debug_log.append(f"Goodinfo錯誤: {e}")

    try:
        url_fubon = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zck/zck_{target_id}.djhtm"
        html = safe_get_fubon(url_fubon)
        
        if html:
            table_match = re.search(r'姓名/法人名稱(.*?)</table>', html, re.IGNORECASE | re.DOTALL)
            if table_match:
                table_html = table_match.group(1)
                trs = re.findall(r'<tr[^>]*>(.*?)</tr>', table_html, re.IGNORECASE | re.DOTALL)
                entity_dict = {}
                for tr in trs:
                    tds = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', tr, re.IGNORECASE | re.DOTALL)
                    if len(tds) >= 4:
                        title = re.sub(r'<[^>]+>', '', tds[0]).strip()
                        name = re.sub(r'<[^>]+>', '', tds[1]).strip()
                        ratio_str = re.sub(r'<[^>]+>', '', tds[3]).replace('%', '').strip()
                        
                        if ('董' in title or '監' in title) and '辭' not in title and '職稱' not in title:
                            try:
                                ratio = float(ratio_str)
                                entity_name = name.split('-')[0].strip() 
                                entity_dict[entity_name] = max(entity_dict.get(entity_name, 0), ratio)
                            except: pass
                
                total_ratio = sum(entity_dict.values())
                if 0 < total_ratio < 100.0:
                    return {}, round(total_ratio, 2), "富邦精算", debug_log
    except Exception as e: 
        debug_log.append(f"富邦錯誤: {e}")

    return {}, 0.0, "失敗", debug_log

def get_dead_chip_info(date_str, dead_chip_input, dynamic_dict, static_val, chip_engine):
    if dead_chip_input and str(dead_chip_input).strip() != "":
        try: return float(str(dead_chip_input).replace('%', '').strip()), "手動"
        except: pass
        
    month_key = str(date_str)[:7].replace('/', '-')
    if dynamic_dict and month_key in dynamic_dict:
        return dynamic_dict[month_key], "Goodinfo當月"
        
    if dynamic_dict and len(dynamic_dict) > 0:
        return list(dynamic_dict.values())[0], "Goodinfo最新"
        
    if static_val > 0:
        return static_val, chip_engine
        
    return 0.0, "-"

# ==========================================
# 鉅額交易掃描 (3日)
# ==========================================
def scrape_block_trades(target_id, actual_dates):
    target_dates = actual_dates[:3] 
    block_data = []
    debug_log = []
    
    def fetch_date(d):
        d_twse = d.replace("-", "")
        d_tpex = f"{int(d.split('-')[0])-1911}/{d.split('-')[1]}/{d.split('-')[2]}"
        res_list = []
        headers = {"User-Agent": "Mozilla/5.0"}
        
        try:
            url = f"https://www.twse.com.tw/rwd/zh/block/BFIAUU?date={d_twse}&response=json"
            res = requests.get(url, headers=headers, timeout=5, verify=False)
            if res.status_code != 200:
                debug_log.append(f"TWSE {res.status_code}")
            else:
                j = res.json()
                if "data" in j and j["data"]:
                    for r in j["data"]:
                        if target_id in str(r): res_list.append([d, "TWSE鉅額", r])
        except: debug_log.append(f"TWSE例外")
            
        try:
            url = f"https://www.tpex.org.tw/www/zh-tw/blockTrade/quote?date={d_tpex}&id=&response=json"
            res = requests.get(url, headers=headers, timeout=5, verify=False)
            if res.status_code != 200:
                debug_log.append(f"TPEx {res.status_code}")
            else:
                j = res.json()
                if "tables" in j and len(j["tables"])>0 and "data" in j["tables"][0]:
                    for r in j["tables"][0]["data"]:
                        if target_id in str(r): res_list.append([d, "TPEx鉅額", r])
                elif "aaData" in j and j["aaData"]:
                    for r in j["aaData"]:
                        if target_id in str(r): res_list.append([d, "TPEx鉅額", r])
        except: debug_log.append(f"TPEx例外")
            
        return res_list

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        for data in executor.map(fetch_date, target_dates):
            if data: block_data.extend(data)
            
    if not block_data:
        uniq_logs = list(set(debug_log))
        return pd.DataFrame(), uniq_logs
        
    parsed = []
    for item in block_data:
        date, src, row = item
        nums = []
        for c in row:
            c_str = re.sub(r'<[^>]+>', '', str(c)).replace(',', '').strip()
            if c_str and ':' not in c_str:
                try: nums.append(float(c_str))
                except: pass
        nums.sort(reverse=True)
        if len(nums) >= 3:
            amt = nums[0] / 10000 if nums[0] > 100000 else nums[0]
            vol = nums[1] / 1000 if nums[1] > 1000 else nums[1]
            price = nums[2]
            
            t_type = "鉅額"
            for c in row:
                if any(x in str(c) for x in ["配對", "交易", "單一", "組合", "逐筆"]):
                    t_type = re.sub(r'<[^>]+>', '', str(c)).strip()
                    break
            parsed.append({
                "日期": date, "交易別": t_type, "成交量(張)": int(vol), 
                "成交價(元)": round(price, 2), "成交金額(萬元)": int(amt)
            })
            
    if not parsed:
        return pd.DataFrame(), ["資料解析失敗"]
        
    df = pd.DataFrame(parsed).sort_values("日期", ascending=False)
    return df, list(set(debug_log))

def process_day_trading(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    df_out = df_out.rename(columns={
        "date": "日期",
        "Volume": "當沖總股數",
        "BuyAfterSale": "先買後賣股數",
        "SellAfterBuy": "先賣後買股數",
        "DayTradingVolume": "當沖總股數"
    })
    for col in ["當沖總股數", "先買後賣股數", "先賣後買股數"]:
        if col in df_out.columns:
            df_out[col.replace('股數', '張數')] = (pd.to_numeric(df_out[col], errors='coerce').fillna(0) / 1000).round().astype(int)
            df_out = df_out.drop(columns=[col])
    
    cols = ['日期'] + [c for c in df_out.columns if '張數' in c or '率' in c]
    df_res = df_out[cols].tail(15).sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

# ==========================================
# 📌 智能門檻計算引擎 (V24.1 嚴謹對齊版)
# ==========================================
def get_smart_threshold(price, capital_bn, dead_float):
    if pd.isna(price) or price <= 0: return 1000 # 防呆
    
    sfc = max(3000, capital_bn * 500)
    si = max(0.1, 0.5 * (100 - dead_float) / 100)
    
    shares_by_money = (sfc * 10000) / (price * 1000)
    shares_by_influence = (capital_bn * 10000) * (si / 100) 
    
    raw_threshold = max(shares_by_money, shares_by_influence)
    
    levels = [100, 200, 400, 600, 800, 1000]
    aligned_threshold = min(levels, key=lambda x: abs(x - raw_threshold))
    
    if price < 30:
        return min(aligned_threshold, 400)
    
    return aligned_threshold

# ==========================================
# 📌 收盤價處理函式
# ==========================================
def process_price(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    df_out['Trading_Volume'] = (pd.to_numeric(df_out['Trading_Volume'], errors='coerce').fillna(0) / 1000).round().astype(int)
    df_out = df_out.rename(columns={"date":"日期","Trading_Volume":"成交量(張)","Trading_money":"成交金額(千元)","open":"開盤價(元)","max":"最高價(元)","min":"最低價(元)","close":"收盤價(元)","spread":"漲跌(元)"})
    df_out["斷頭價(0.78)"] = (df_out["收盤價(元)"] * 0.78).round(2)
    df_res = df_out[['日期','成交量(張)','開盤價(元)','最高價(元)','最低價(元)','收盤價(元)','漲跌(元)','斷頭價(0.78)']].sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

# ==========================================
# 資料處理引擎 (分點與家數差)
# ==========================================
def fetch_single_day_branch(d):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id, "start_date": d, "end_date": d}
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=15).json()
        return res.get("data", [])
    except: return []

def fetch_fm_branch_fast_parallel(dates_list):
    all_data = []
    progress_bar = st.progress(0); status_text = st.empty()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        f_to_d = {executor.submit(fetch_single_day_branch, d): d for d in dates_list}
        completed = 0
        for f in concurrent.futures.as_completed(f_to_d):
            completed += 1; status_text.text(f"📥 下載分點資料... ({completed}/{len(dates_list)})"); progress_bar.progress(completed / len(dates_list))
            if f.result(): all_data.extend(f.result())
    status_text.empty(); progress_bar.empty()
    return pd.DataFrame(all_data)

def process_branch_diff(df_raw, actual_dates):
    if df_raw.empty: return pd.DataFrame()
    out = []
    for d in actual_dates[:15]:
        df_day = df_raw[df_raw['date'] == d]
        if df_day.empty: continue
        buy_count = df_day[df_day['buy'] > 0]['securities_trader'].nunique()
        sell_count = df_day[df_day['sell'] > 0]['securities_trader'].nunique()
        out.append({"日期": d, "買進家數": buy_count, "賣出家數": sell_count, "買賣家數差": buy_count - sell_count})
    df_out = pd.DataFrame(out)
    if not df_out.empty: df_out.columns = list(df_out.columns)
    return df_out

def process_branch_top15(df_raw, period_days, actual_dates):
    if len(actual_dates) < period_days or df_raw.empty: return pd.DataFrame()
    target_dates = actual_dates[:period_days]
    df_period = df_raw[df_raw['date'].isin(target_dates)].copy()
    if df_period.empty: return pd.DataFrame()
    df_period['buy'] = (pd.to_numeric(df_period['buy'], errors='coerce').fillna(0) / 1000).round().astype(int)
    df_period['sell'] = (pd.to_numeric(df_period['sell'], errors='coerce').fillna(0) / 1000).round().astype(int)
    g = df_period.groupby('securities_trader')[['buy', 'sell']].sum().reset_index()
    g['net'] = g['buy'] - g['sell']
    buyers = g[g['net'] > 0].sort_values('net', ascending=False).reset_index(drop=True)
    sellers = g[g['net'] < 0].sort_values('net', ascending=True).reset_index(drop=True)
    total_vol = g['buy'].sum() if g['buy'].sum() > 0 else 1
    out = pd.DataFrame()
    max_len = 15
    b_n, b_i, b_o, b_net, b_pct, s_n, s_i, s_o, s_net, s_pct = [], [], [], [], [], [], [], [], [], []
    for i in range(max_len):
        if i < len(buyers):
            b_n.append(buyers.loc[i, 'securities_trader']); b_i.append(int(buyers.loc[i, 'buy'])); b_o.append(int(buyers.loc[i, 'sell'])); b_net.append(int(buyers.loc[i, 'net'])); b_pct.append(f"{(buyers.loc[i, 'net']/total_vol)*100:.2f}%")
        else: b_n.append("-"); b_i.append(0); b_o.append(0); b_net.append(0); b_pct.append("-")
        if i < len(sellers):
            s_n.append(sellers.loc[i, 'securities_trader']); s_i.append(int(sellers.loc[i, 'buy'])); s_o.append(int(sellers.loc[i, 'sell'])); s_net.append(abs(int(sellers.loc[i, 'net']))); s_pct.append(f"{(abs(sellers.loc[i, 'net'])/total_vol)*100:.2f}%")
        else: s_n.append("-"); s_i.append(0); s_o.append(0); s_net.append(0); s_pct.append("-")
    out["買超分點"]=b_n; out["買進(張)"]=b_i; out["賣出(張)"]=b_o; out["買超(張)"]=b_net; out["佔比"]=b_pct
    out["賣超分點"]=s_n; out["買進(張)."]=s_i; out["賣出(張)."]=s_o; out["賣超(張)"]=s_net; out["佔比."]=s_pct
    out.columns = list(out.columns)
    return out

# ==========================================
# 質押與鉅額 
# ==========================================
def extract_fubon_table(html_text, trigger, cols):
    start_idx = html_text.find(trigger)
    if start_idx == -1: return []
    fast_html = html_text[max(0, start_idx - 500) : start_idx + 35000]
    tr_pattern = re.compile(r'<tr[^>]*>([\s\S]*?)</tr>', re.IGNORECASE)
    td_pattern = re.compile(r'<t[dh][^>]*>([\s\S]*?)</t[dh]>', re.IGNORECASE)
    trs = tr_pattern.findall(fast_html)
    out = []
    is_t = False
    for tr in trs:
        tds = td_pattern.findall(tr)
        if tds:
            row = [re.sub(r'<[^>]+>', '', td).replace('&nbsp;', '').replace(' ', '').replace('\r', '').replace('\n', '').strip() for td in tds]
            row_str = "".join(row)
            if trigger in row_str: is_t = True
            elif is_t and len(row) >= cols:
                if row[0] == "" or "註" in row[0]: is_t = False
                else: out.append(row[:cols])
    return out

def scrape_fubon_pledge(df_price_raw):
    all_data = []
    for i in range(3):
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zc0/zc06_{stock_id}_{i}.djhtm"
        html = safe_get_fubon(url)
        if html:
            p = extract_fubon_table(html, "設質人身", 7)
            if p: all_data.extend(p)

    if not all_data: return pd.DataFrame(), pd.DataFrame()
    seen = set(); uniq_data = []
    for r in all_data:
        if "|".join(r) not in seen: seen.add("|".join(r)); uniq_data.append(r)
    df_all = pd.DataFrame(uniq_data, columns=["日期", "身份別", "姓名", "設質(張)", "解質(張)", "累積質設(張)", "質權人"])
    current_year, current_month = datetime.datetime.now().year, datetime.datetime.now().month
    pledge_cur_y, pledge_last_m = current_year, 99
    parsed_dates = []
    for d_str in df_all['日期']:
        if len(d_str) == 5 and '/' in d_str: 
            m = int(d_str.split('/')[0])
            if pledge_last_m == 99: pledge_cur_y = current_year - 1 if m > current_month + 1 and current_month < 3 else current_year
            elif m > pledge_last_m + 1: pledge_cur_y -= 1
            pledge_last_m = m
            parsed_dates.append(f"{pledge_cur_y}-{d_str.replace('/', '-')}")
        elif len(d_str) >= 7 and '/' in d_str: 
            pts = d_str.split('/')
            y = int(pts[0]) + 1911
            pledge_cur_y, pledge_last_m = y, int(pts[1])
            parsed_dates.append(f"{y}-{pts[1]}-{pts[2]}")
        else: parsed_dates.append(d_str)
    df_all['日期'] = parsed_dates
    for col in ["設質(張)", "解質(張)", "累積質設(張)"]:
        df_all[col] = pd.to_numeric(df_all[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
    price_dict = {pd.to_datetime(row['date']).strftime('%Y-%m-%d'): row['close'] for _, row in df_price_raw.iterrows()}
    pledge_prices, margin_calls = [], []
    for _, row in df_all.iterrows():
        d_str, sz = row['日期'], row['設質(張)']
        found_p, mc = "-", "-"
        if sz > 0:
            try:
                target_d = pd.to_datetime(d_str)
                for i in range(20):
                    check_d = (target_d - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                    if check_d in price_dict:
                        found_p = price_dict[check_d]; mc = round(found_p * 0.78, 2); break
            except: pass
        pledge_prices.append(found_p); margin_calls.append(mc)
    df_all['設質日收盤價'], df_all['強制賣出價(0.78)'] = pledge_prices, margin_calls
    summary_map = {}
    for _, r in df_all.iterrows():
        name = r['姓名']
        if name not in summary_map: summary_map[name] = {"title": r['身份別'], "balance": r['累積質設(張)'], "p": "-", "mc": "-"}
        if summary_map[name]["p"] == "-" and r['設質(張)'] > 0:
            summary_map[name]["p"] = r['設質日收盤價']; summary_map[name]["mc"] = r['強制賣出價(0.78)']
    summary_rows = [{"身份別": d["title"], "姓名": n, "目前剩餘質設(張)": d["balance"], "最後設質收盤價(元)": d["p"], "估算斷頭價(0.78)": d["mc"]} for n, d in summary_map.items() if d["balance"] > 0]
    
    df_sum_out = pd.DataFrame(summary_rows)
    if not df_sum_out.empty: df_sum_out.columns = list(df_sum_out.columns)
    if not df_all.empty: df_all.columns = list(df_all.columns)
    return df_sum_out, df_all

# ==========================================
# 集保處理引擎 (橫向加總)
# ==========================================
def clean_level_by_math(x):
    s = str(x).replace(',', '').replace(' ', '')
    if s in ["17", "17.0", "合計", "總計"]: return "合計"
    nums = re.findall(r'\d+', s)
    if not nums: return s
    if len(nums) == 1 and int(nums[0]) <= 15:
        m = {1: "1-999股", 2: "1-5張", 3: "5-10張", 4: "10-15張", 5: "15-20張", 6: "20-30張", 7: "30-40張", 8: "40-50張", 9: "50-100張", 10: "100-200張", 11: "200-400張", 12: "400-600張", 13: "600-800張", 14: "800-1000張", 15: "1000張以上"}
        return m.get(int(nums[0]), s)
    up = int(nums[-1])
    if up <= 999: return "1-999股"
    elif up <= 5000: return "1-5張"
    elif up <= 10000: return "5-10張"
    elif up <= 15000: return "10-15張"
    elif up <= 20000: return "15-20張"
    elif up <= 30000: return "20-30張"
    elif up <= 40000: return "30-40張"
    elif up <= 50000: return "40-50張"
    elif up <= 100000: return "50-100張"
    elif up <= 200000: return "100-200張"
    elif up <= 400000: return "200-400張"
    elif up <= 600000: return "400-600張"
    elif up <= 800000: return "600-800張"
    elif up <= 1000000: return "800-1000張"
    else: return "1000張以上"

def process_tdcc(df):
    if df.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df = df[~df['HoldingSharesLevel'].astype(str).str.contains('差異數')]
    df['LevelClean'] = df['HoldingSharesLevel'].apply(clean_level_by_math)
    df['people'] = pd.to_numeric(df['people'], errors='coerce').fillna(0).astype(int)
    df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0)
    df['unit'] = (pd.to_numeric(df.get('unit', 0), errors='coerce').fillna(0) / 1000).round().astype(int)
    
    dates = sorted(df['date'].unique(), reverse=True)[:10]
    df = df[df['date'].isin(dates)]
    df_levels = df[~df['LevelClean'].str.contains('合計|總計')]
    
    p_unit = df_levels.pivot_table(index='date', columns='LevelClean', values='unit', aggfunc='first').reset_index().fillna(0)
    p_people = df_levels.pivot_table(index='date', columns='LevelClean', values='people', aggfunc='first').reset_index().fillna(0)
    p_pct = df_levels.pivot_table(index='date', columns='LevelClean', values='percent', aggfunc='first').reset_index().fillna(0)
    
    lvls = ['1-999股', '1-5張', '5-10張', '10-15張', '15-20張', '20-30張', '30-40張', '40-50張', '50-100張', '100-200張', '200-400張', '400-600張', '600-800張', '800-1000張', '1000張以上']
    for l in lvls:
        if l not in p_unit.columns: p_unit[l] = 0
        if l not in p_people.columns: p_people[l] = 0
        if l not in p_pct.columns: p_pct[l] = 0

    df_total = pd.DataFrame({'date': p_unit['date']})
    df_total['總張數'] = p_unit[lvls].sum(axis=1)
    df_total['總人數(人)'] = p_people[lvls].sum(axis=1)
    df_total['總均張'] = df_total.apply(lambda r: round(r['總張數']/r['總人數(人)'], 2) if r['總人數(人)']>0 else 0, axis=1)
    
    df_wide = df_total.copy()
    for l in lvls:
        df_wide[f"{l}_張數"] = p_unit[l]
        df_wide[f"{l}_人數"] = p_people[l]
        df_wide[f"{l}_比例(%)"] = p_pct[l]
    df_wide = df_wide.rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    df_unit = pd.merge(df_total[['date', '總張數']], p_unit[['date']+lvls], on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    df_people = pd.merge(df_total[['date', '總人數(人)']], p_people[['date']+lvls], on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    df_percent = p_pct[['date']+lvls].rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    df_avg_base = pd.DataFrame({'date': p_unit['date']})
    for l in lvls: df_avg_base[l] = (p_unit[l] / p_people[l].replace(0, pd.NA)).fillna(0).round(2)
    df_avg = pd.merge(df_total[['date', '總均張']], df_avg_base, on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    df_wide.columns = list(df_wide.columns); df_unit.columns = list(df_unit.columns); df_people.columns = list(df_people.columns)
    df_percent.columns = list(df_percent.columns); df_avg.columns = list(df_avg.columns)
    
    return df_wide, df_unit, df_people, df_percent, df_avg

def process_tdcc_dynamic(df_share_wide, df_price, dead_chip_input, dynamic_dict, static_val, chip_engine):
    if df_share_wide.empty or df_price.empty: return pd.DataFrame()
    
    df_s = df_share_wide.copy()
    df_p = df_price.copy()
    df_s['dt'] = pd.to_datetime(df_s['日期'])
    df_p['dt'] = pd.to_datetime(df_p['日期'])
    
    df_m = pd.merge_asof(df_s.sort_values('dt'), df_p.sort_values('dt')[['dt', '收盤價(元)']], on='dt', direction='backward').sort_values('dt', ascending=False)
    
    out = []
    for _, row in df_m.iterrows():
        p = row.get('收盤價(元)', 0)
        d_str = row['日期']
        if pd.isna(p) or p == 0: continue
        
        current_dead_chip, chip_label = get_dead_chip_info(d_str, dead_chip_input, dynamic_dict, static_val, chip_engine)
        cap_bn = row.get('總張數', 0) / 10000
        ceiling_t = get_smart_threshold(p, cap_bn, current_dead_chip)
        
        l_cols = []
        if ceiling_t <= 100: l_cols = ['100-200張_比例(%)', '200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 200: l_cols = ['200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 400: l_cols = ['400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 600: l_cols = ['600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 800: l_cols = ['800-1000張_比例(%)', '1000張以上_比例(%)']
        else: l_cols = ['1000張以上_比例(%)']

        l_pct = sum([pd.to_numeric(row.get(c, 0), errors='coerce') for c in l_cols])
        
        c_display, status = "-", "無死籌碼數據"
        if 0 < current_dead_chip < 100:
            c_val = max(0, (l_pct - current_dead_chip) / (100.0 - current_dead_chip))
            status = "🔴 絕對控盤" if c_val >= 0.5 else "🟡 高度鎖碼" if c_val >= 0.3 else "🔵 初步集結" if c_val >= 0.15 else "⚪ 籌碼渙散"
            c_display = round(c_val * 100, 2)

        out.append({
            "日期": d_str, 
            "收盤價(元)": p, 
            "股本(億)": round(cap_bn, 2),
            "主導門檻": f"智能精算 ({int(ceiling_t)}張)",
            "級距總佔比(%)": round(l_pct, 2),
            "死籌碼(%)": f"{float(current_dead_chip):.2f}% ({chip_label})" if current_dead_chip > 0 else "-",
            "活大戶C_Value(%)": c_display,
            "實戰判定": status
        })
        
    out_df = pd.DataFrame(out)
    if not out_df.empty: out_df.columns = list(out_df.columns)
    return out_df

def get_expert_advice_v24(row, dead_chip_input, dynamic_dict, static_val):
    advice = []
    if pd.isna(row.get('1000張變動(%)')): return "⚪ 數據初始化..."
    
    current_dead_chip, _ = get_dead_chip_info(row['日期'], dead_chip_input, dynamic_dict, static_val, "")
    leverage = 100 / (100 - current_dead_chip) if 0 < current_dead_chip < 100 else 1
    
    real_1000_change = row['1000張變動(%)'] * leverage
    real_combat_change = row['作戰區變動(%)'] * leverage
    max_intensity = real_1000_change if abs(real_1000_change) > abs(real_combat_change) else real_combat_change

    price = row.get('收盤價(元)', 0)

    if 0 < price < 30 and row['1000張變動(%)'] >= 1.0:
        advice.append(f"💎 [鐵桿鎖碼] 頂層真身大幅上揚，強度 {real_1000_change:.2f}%")

    if row['總人數變動率(%)'] > 2.0 and (real_1000_change < -0.5 or real_combat_change < -0.5):
        advice.append(f"💀 [逃命警報] 散戶爆量接刀，活籌碼流出強度 {abs(max_intensity):.2f}%")
        return " | ".join(advice) 

    if max_intensity > 3.0 and row['總人數變動率(%)'] < 0:
        advice.append(f"🚀 [暴力軋空] 活籌碼強勢壓縮 {max_intensity:.2f}%")

    if row.get('中實戶人數變動', 0) >= 2 and 200 <= row.get('K_Value', 0) <= 600:
        advice.append(f"🔴 [分身集結] 偵測到中層主力施工，K值({row['K_Value']})")

    if row.get('中實戶人數變動', -1) == 0 and real_combat_change >= 0.5:
        advice.append("🔥 [定員增持] 原班人馬持續加壓！")

    if row['總人數變動率(%)'] > 1.5 and real_1000_change >= -0.1 and real_combat_change >= -0.1:
        advice.append("🟣 [惡意甩轎] 散戶湧入但主力未退，刻意讓道洗盤")

    return " | ".join(advice) if advice else "🔵 趨勢盤整/無明顯訊號"

def process_v24_ultimate_radar(df_wide, dead_chip_input, dynamic_dict, static_val, df_price):
    if df_wide.empty or len(df_wide) < 2: return pd.DataFrame()
    
    df = df_wide.sort_values('日期', ascending=True).copy()
    
    df['dt_end'] = pd.to_datetime(df['日期'])
    df_p = df_price.copy()
    if not df_p.empty and '日期' in df_p.columns:
        df_p['dt'] = pd.to_datetime(df_p['日期'])
        df = pd.merge_asof(df, df_p.sort_values('dt')[['dt', '收盤價(元)']], left_on='dt_end', right_on='dt', direction='backward')
    else:
        df['收盤價(元)'] = 0
        
    df['中實戶人數'] = df.get('200-400張_人數', 0)
    df['中實戶總數'] = df.get('200-400張_張數', 0)
    
    df['核心區佔比(%)'] = df.get('400-600張_比例(%)',0) + df.get('600-800張_比例(%)',0) + df.get('800-1000張_比例(%)',0) + df.get('1000張以上_比例(%)',0)
    df['作戰區佔比(%)'] = df.get('200-400張_比例(%)',0) + df.get('400-600張_比例(%)',0) + df.get('600-800張_比例(%)',0)
    
    df['總人數變動率(%)'] = (df['總人數(人)'].pct_change() * 100).round(2)
    df['1000張變動(%)'] = df.get('1000張以上_比例(%)', pd.Series([0]*len(df))).diff().round(2)
    df['作戰區變動(%)'] = df['作戰區佔比(%)'].diff().round(2)
    df['中實戶人數變動'] = df['中實戶人數'].diff()
    df['中實戶張數變動'] = df['中實戶總數'].diff()
    
    def calc_k(row):
        if row['中實戶人數變動'] >= 2 and row['中實戶張數變動'] > 0:
            return round(row['中實戶張數變動'] / row['中實戶人數變動'], 2)
        return 0.0
    df['K_Value'] = df.apply(calc_k, axis=1)
    
    df['V24_實戰診斷'] = df.apply(lambda row: get_expert_advice_v24(row, dead_chip_input, dynamic_dict, static_val), axis=1)
    
    report_columns = ['日期', '收盤價(元)', '總人數變動率(%)', '1000張變動(%)', '作戰區變動(%)', 'K_Value', 'V24_實戰診斷']
    final_report = df[report_columns].sort_values('日期', ascending=False).fillna(0).head(10)
    
    return final_report

if run_btn:
    with st.spinner(f"正在擷取 {stock_id} 數據，並啟動 V24.2 終極防呆雷達..."):
        
        stock_name = get_stock_name(stock_id)
        
        start_probe = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y-%m-%d")
        df_p_raw = fetch_fm("TaiwanStockPrice", start_probe)
        if df_p_raw.empty: st.error("查無股價資料"); st.stop()
        
        actual_dates = sorted(df_p_raw['date'].unique().tolist(), reverse=True)
        d_60 = actual_dates[59] if len(actual_dates) >= 60 else actual_dates[-1]
        
        df_price = process_price(df_p_raw)
        
        dynamic_dict, static_val, chip_engine, debug_log = scrape_director_holding(stock_id)
        
        if not (dead_chip_input and str(dead_chip_input).strip() != "") and len(dynamic_dict) == 0 and static_val == 0:
            st.error(f"⚠️ 死籌碼引擎全滅！可能遭防火牆阻擋。連線紀錄: {', '.join(debug_log)}。請於上方手動輸入死籌碼！")

        df_branch_raw = fetch_fm_branch_fast_parallel(actual_dates[:60])
        df_branch_diff = process_branch_diff(df_branch_raw, actual_dates)
        
        df_share_raw = fetch_fm("TaiwanStockHoldingSharesPer", d_60)
        df_share_wide, df_share_unit, df_share_people, df_share_pct, df_share_avg = process_tdcc(df_share_raw)
        
        df_share_dynamic = process_tdcc_dynamic(df_share_wide, df_price, dead_chip_input, dynamic_dict, static_val, chip_engine)
        df_v24_radar = process_v24_ultimate_radar(df_share_wide, dead_chip_input, dynamic_dict, static_val, df_price)
        
        df_twse, twse_log = scrape_block_trades(stock_id, actual_dates)
        df_margin = process_margin(fetch_fm("TaiwanStockMarginPurchaseShortSale", d_60))
        df_day_trade = process_day_trading(fetch_fm("TaiwanStockDayTrading", d_60)) 
        df_inst = process_inst(fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", d_60))
        
        df_rev_raw = fetch_fm("TaiwanStockMonthRevenue", "2022-01-01")
        df_rev = pd.DataFrame()
        if not df_rev_raw.empty:
            df_rev_raw['營收月份'] = df_rev_raw['revenue_year'].astype(str) + "-" + df_rev_raw['revenue_month'].astype(str).str.zfill(2)
            df_rev = df_rev_raw.rename(columns={"revenue":"月營收(百萬元)"})[['營收月份','月營收(百萬元)']].tail(24)
            df_rev['月營收(百萬元)'] = (df_rev['月營收(百萬元)']/1000000).round().astype(int)
            df_rev.columns = list(df_rev.columns)
        
        df_b_today = process_branch_top15(df_branch_raw, 1, actual_dates)
        df_b_prev1 = process_branch_top15(df_branch_raw, 1, actual_dates[1:])
        df_b_3 = process_branch_top15(df_branch_raw, 3, actual_dates)
        df_b_10 = process_branch_top15(df_branch_raw, 10, actual_dates)
        df_b_20 = process_branch_top15(df_branch_raw, 20, actual_dates)
        df_b_30 = process_branch_top15(df_branch_raw, 30, actual_dates)
        df_b_60 = process_branch_top15(df_branch_raw, 60, actual_dates)

        df_gov = pd.DataFrame()
        if not df_b_today.empty:
            govs = ["台銀", "土銀", "彰銀", "第一", "兆豐", "華南", "合庫", "台企銀"]
            df_gov = df_b_today[df_b_today.astype(str).apply(lambda x: x.str.contains('|'.join(govs))).any(axis=1)]
            df_gov.columns = list(df_gov.columns)

        df_pledge_summary, df_pledge_detail = scrape_fubon_pledge(df_p_raw)
        df_fut = process_fut_inst(fetch_fm("TaiwanFuturesInstitutionalInvestors", d_60, specific_id=False, target_id="TX"))
        df_div = process_div(fetch_fm("TaiwanStockDividend", "2015-01-01"))
        df_per = process_per(fetch_fm("TaiwanStockPER", d_60))
        df_disp = process_disp(fetch_fm("TaiwanStockDispositionSecuritiesPeriod", (datetime.date.today()-datetime.timedelta(days=180)).strftime("%Y-%m-%d")))
        df_cbas_raw = fetch_fm("TaiwanStockConvertibleBondDailyOverview", actual_dates[0], specific_id=False)
        df_cbas = process_cbas(df_cbas_raw[df_cbas_raw['cb_id'].astype(str).str.startswith(stock_id)]) if not df_cbas_raw.empty else pd.DataFrame()
        df_opt_inst = process_opt_inst(fetch_fm("TaiwanOptionInstitutionalInvestors", d_60, specific_id=False, target_id="TXO"))

        def show(title, df, custom_class=""):
            st.markdown(f"#### {title}")
            if df is None or df.empty: 
                st.warning("此區塊查無數據或無發行紀錄")
            else:
                def fmt_int(x):
                    if pd.isna(x): return "-"
                    s = str(x).strip()
                    if s == "-" or s == "": return "-"
                    is_pct = "%" in s
                    try:
                        v = float(s.replace(",", "").replace("%", ""))
                        return f"{int(v):,}" + ("%" if is_pct else "")
                    except: return str(x)
                    
                def fmt_float(x):
                    if pd.isna(x): return "-"
                    s = str(x).strip()
                    if s == "-" or s == "": return "-"
                    is_pct = "%" in s
                    try:
                        v = float(s.replace(",", "").replace("%", ""))
                        return f"{v:,.2f}" + ("%" if is_pct else "")
                    except: return str(x)

                def fmt_auto(x):
                    if pd.isna(x): return "-"
                    if isinstance(x, (int, np.integer)): return f"{int(x):,}"
                    if isinstance(x, (float, np.floating)): return f"{float(x):,.2f}"
                    return str(x)

                format_dict = {}
                for c in df.columns:
                    if any(kw in c for kw in ['人數', '張數', '股數', '口', '次數', '家數', '金額', '量']):
                        format_dict[c] = fmt_int
                    elif any(kw in c for kw in ['比', '價', '率', '值', '報酬', 'C(%)']):
                        format_dict[c] = fmt_float
                    else:
                        format_dict[c] = fmt_auto

                left_cols = [c for c in df.columns if any(kw in str(c) for kw in ['分點', '名稱', '姓名', '身份別', '質權人', '交易別', '診斷', '判定', '門檻', '條件', '措施', '契約', '代號', '來源'])]
                right_cols = [c for c in df.columns if c not in left_cols]

                styler = df.style.format(format_dict)
                styler = styler.set_properties(**{'text-align': 'right !important'}, subset=right_cols)
                if left_cols:
                    styler = styler.set_properties(**{'text-align': 'left !important'}, subset=left_cols)

                try: styler = styler.hide(axis="index")
                except: styler = styler.hide_index()
                
                styler = styler.set_table_styles([
                    dict(selector='th', props=[('text-align', 'center !important')]),
                    dict(selector='table', props=[('width', '100%')])
                ])
                
                html = styler.to_html()
                if custom_class: html = html.replace('<table', f'<table class="{custom_class}"')
                st.markdown(html, unsafe_allow_html=True)
            
        show("▼▼▼ 1-1. 雙軸活大戶鎖碼判定表 (C-Value) ▼▼▼", df_share_dynamic)
        show("▼▼▼ 1-2. 專家診斷雷達 ▼▼▼", df_v24_radar, custom_class="radar-table")
        show("▼▼▼ 2-1. 集保分級 - 張數表 (近10週) ▼▼▼", df_share_unit)
        show("▼▼▼ 2-2. 集保分級 - 人數表 (近10週) ▼▼▼", df_share_people)
        show("▼▼▼ 2-3. 集保分級 - 比例表 (%) ▼▼▼", df_share_pct)
        show("▼▼▼ 2-4. 集保分級 - 均張表 ▼▼▼", df_share_avg)
        
        if df_twse.empty:
            st.markdown(f"#### ▼▼▼ 3. 鉅額交易明細 (近3日) [來源：證交所/櫃買中心] ▼▼▼")
            err_msg = ", ".join(twse_log) if twse_log else "本檔股票近 3 營業日無鉅額交易紀錄"
            st.warning(err_msg)
        else:
            show("▼▼▼ 3. 鉅額交易明細 (近3日) [來源：證交所/櫃買中心] ▼▼▼", df_twse)
            
        show("▼▼▼ 4. 散戶資券餘額 [來源：FinMind] ▼▼▼", df_margin)
        show("▼▼▼ 5. 現股當沖明細 [來源：FinMind] ▼▼▼", df_day_trade)
        show("▼▼▼ 6. 法人買賣超 [來源：FinMind] ▼▼▼", df_inst)
        show("▼▼▼ 7. 收盤價量 [來源：FinMind] ▼▼▼", df_price.head(15))
        show("▼▼▼ 8. 月營收 (百萬元) ▼▼▼", df_rev)
        
        show(f"▼▼▼ 9. 主力分點 - 今日 ({actual_dates[0]}) [來源：FinMind] ▼▼▼", df_b_today)
        show(f"▼▼▼ 10. 主力分點 - 前一日 ({actual_dates[1] if len(actual_dates)>1 else '無'}) [來源：FinMind] ▼▼▼", df_b_prev1)
        show("▼▼▼ 11. 主力分點 - 近3日 [來源：FinMind] ▼▼▼", df_b_3)
        show("▼▼▼ 12. 主力分點 - 近10日 [來源：FinMind] ▼▼▼", df_b_10)
        show("▼▼▼ 13. 主力分點 - 近20日 [來源：FinMind] ▼▼▼", df_b_20)
        show("▼▼▼ 14. 主力分點 - 近30日 [來源：FinMind] ▼▼▼", df_b_30)
        show("▼▼▼ 15. 主力分點 - 近60日 [來源：FinMind] ▼▼▼", df_b_60)
        show("▼▼▼ 16. 八大官股進出 (今日) [來源：FinMind] ▼▼▼", df_gov)
        show("▼▼▼ 17. 買賣家數差明細 (近15日) [來源：系統自算] ▼▼▼", df_branch_diff)
        
        st.markdown("#### ▼▼▼ 18. 董監大股東質設明細 [來源：富邦證券] ▼▼▼")
        if df_pledge_detail.empty: st.warning("此區塊查無數據")
        else:
            if not df_pledge_summary.empty: st.markdown(df_pledge_summary.to_html(index=False, border=1), unsafe_allow_html=True)
            st.markdown(df_pledge_detail.to_html(index=False, border=1), unsafe_allow_html=True)
            
        show("▼▼▼ 19. 台指期貨三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_fut)
        show("▼▼▼ 20. 台指選擇權三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_opt_inst)
        show("▼▼▼ 21. 歷年股利 [來源：FinMind] ▼▼▼", df_div)
        show("▼▼▼ 22. 本益比、淨值比與殖利率 [來源：FinMind] ▼▼▼", df_per)
        show("▼▼▼ 23. 處置有價證券狀態 [來源：FinMind] ▼▼▼", df_disp)
        show("▼▼▼ 24. CBAS 可轉債數據 [來源：FinMind] ▼▼▼", df_cbas)

        st.divider(); st.subheader("📋 【給 Gemini 的量化分析資料包】")
        
        name_str = f" {stock_name}" if stock_name else ""
        p = f"請依下面最新的盤後資料幫我分析 {stock_id}{name_str} 的量化籌碼，必須以我給的資料優先使用。\n\n"
        
        p += format_to_gas(df_share_dynamic, "1-1. 雙軸活大戶鎖碼判定表 (C-Value)")
        p += format_to_gas(df_v24_radar, "1-2. 專家診斷雷達")
        p += format_to_gas(df_share_unit, "2-1. 集保分級 - 張數表")
        p += format_to_gas(df_share_people, "2-2. 集保分級 - 人數表")
        p += format_to_gas(df_share_pct, "2-3. 集保分級 - 比例表 (%)")
        p += format_to_gas(df_share_avg, "2-4. 集保分級 - 均張表")
        p += format_to_gas(df_twse, "3. 鉅額交易明細 (近3日)")
        p += format_to_gas(df_margin, "4. 散戶資券餘額")
        p += format_to_gas(df_day_trade, "5. 現股當沖明細")
        p += format_to_gas(df_inst, "6. 法人買賣超")
        p += format_to_gas(df_price.head(15), "7. 收盤價量")
        p += format_to_gas(df_rev, "8. 月營收 (百萬元)")
        p += format_to_gas(df_b_today, f"9. 主力分點 - 今日 ({actual_dates[0]})")
        p += format_to_gas(df_b_prev1, "10. 主力分點 - 前一日")
        p += format_to_gas(df_b_3, "11. 主力分點 - 近3日")
        p += format_to_gas(df_b_10, "12. 主力分點 - 近10日")
        p += format_to_gas(df_b_20, "13. 主力分點 - 近20日")
        p += format_to_gas(df_b_30, "14. 主力分點 - 近30日")
        p += format_to_gas(df_b_60, "15. 主力分點 - 近60日")
        p += format_to_gas(df_gov, "16. 八大官股進出 (今日)")
        p += format_to_gas(df_branch_diff, "17. 買賣家數差明細 (近15日)")
        p += format_pledge_to_gas(df_pledge_summary, df_pledge_detail)
        p += format_to_gas(df_fut, "19. 台指期貨三大法人未平倉 (大盤)")
        p += format_to_gas(df_opt_inst, "20. 台指選擇權三大法人未平倉 (大盤)")
        p += format_to_gas(df_div, "21. 歷年股利")
        p += format_to_gas(df_per, "22. 本益比、淨值比與殖利率")
        p += format_to_gas(df_disp, "23. 處置有價證券狀態")
        p += format_to_gas(df_cbas, "24. CBAS 可轉債數據")
        
        st.code(p, language="text")
