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
st.set_page_config(page_title="台股全息量化系統 (V15.2 絕對破壁版)", layout="wide")

# 內建最新 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0xMCAyMDoyMDo0NiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.7s3-IrkfdiUyTvGiZQGESBUBAPHQTnd4pwYcn8_J-CY"

# 📌 注入全局 CSS
st.markdown("""
<style>
table.dataframe td { text-align: right !important; }
table.dataframe th { text-align: center !important; }
table.radar-table td:last-child { text-align: left !important; }
</style>
""", unsafe_allow_html=True)

st.title("🤖 交易員實戰手冊：全息量化擷取系統")
st.markdown("✅ **V15.2 專家雷達 (高敏防雷)** | ✅ **富邦 SSL 降級破壁演算法** | ✅ **當沖數據回歸**")

# UI 輸入區
col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
with col1:
    stock_id = st.text_input("個股代號", value="7711")
with col2:
    dead_chip_input = st.text_input("死籌碼 %", placeholder="不填則自動從富邦精算")
with col3:
    money_input = st.text_input("財力設定(萬)", value="5000")
with col4:
    influence_input = st.text_input("影響力設定(%)", value="0.5")

st.write("")
run_btn = st.button("🚀 啟動引擎：擷取全息資料並產生 Prompt", use_container_width=True)

st.divider()

# ==========================================
# 工具函式與四引擎爬蟲
# ==========================================

def safe_get_fubon(url):
    """📌 專門對付富邦老舊 SSL 憑證的降級爬蟲器"""
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        # 繞過 OpenSSL 3.0 的嚴格限制
        if hasattr(ssl, 'OP_LEGACY_SERVER_CONNECT'):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            return response.read().decode('big5', errors='ignore')
    except Exception as e:
        # 備援：如果 urllib 失敗，改用 requests
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
    """📌 V15.2 死籌碼引擎：完美移植 GAS 解析邏輯 + SSL 破壁"""
    headers = {"User-Agent": "Mozilla/5.0"}
    debug_log = []
    
    # 📌 優先權 1：富邦 ZCK (法人代表去重精算法 + SSL 破壁)
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
                    return round(total_ratio, 2), "富邦法人精算", debug_log
    except Exception as e: 
        debug_log.append(f"富邦錯誤: {e}")
    
    # 📌 優先權 2：Goodinfo 備援 (修正 int iterable 報錯)
    try:
        url_good = f"https://goodinfo.tw/tw/StockDirectorSharehold.asp?STOCK_ID={target_id}"
        headers_good = headers.copy()
        headers_good["Referer"] = f"https://goodinfo.tw/tw/StockDetail.asp?STOCK_ID={target_id}"
        headers_good["Cookie"] = "CLIENT_KEY=20260411;" 
        res = requests.get(url_good, headers=headers_good, timeout=8)
        res.encoding = 'utf-8'
        dfs = pd.read_html(StringIO(res.text))
        for df in dfs:
            if isinstance(df.columns, pd.MultiIndex):
                cols = ['_'.join(str(c) for c in col if 'Unnamed' not in str(c)).strip('_') for col in df.columns.values]
                df.columns = cols
            else:
                df.columns = df.columns.astype(str) # 📌 強制將欄位轉為字串，防呆 TypeError
            
            target_col = [c for c in df.columns if '全體董監持股' in c and '持股(%)' in c.replace(' ', '')]
            if target_col:
                for val in df[target_col[0]]:
                    if str(val).strip() not in ['-', '', 'nan']:
                        try:
                            parsed_val = float(str(val).strip())
                            if 0 < parsed_val < 100.0: return parsed_val, "Goodinfo", debug_log
                        except: pass
    except Exception as e: 
        debug_log.append(f"Goodinfo錯誤: {e}")
    
    # 📌 優先權 3：玩股網 API
    try:
        url_wantgoo = f"https://www.wantgoo.com/stock/api/company/profile?StockNo={target_id}"
        res = requests.get(url_wantgoo, headers=headers, timeout=5).json()
        val = float(res.get('directorHoldRatio', 0))
        if 0 < val < 100.0: return val, "玩股網", debug_log
    except: pass

    # 📌 優先權 4：鉅亨網
    try:
        url_cnyes = f"https://ws.cnyes.com/web/api/v1/page/normal/stock/TWS/{target_id}/profile"
        res = requests.get(url_cnyes, headers=headers, timeout=5).json()
        val = float(res['data']['profile']['directorHoldPercent'])
        if 0 < val < 100.0: return val, "鉅亨網", debug_log
    except: pass
    
    return 0.0, "失敗", debug_log

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
            b_n.append(buyers.loc[i, 'securities_trader']); b_i.append(int(buyers.loc[i, 'buy'])); b_o.append(int(buyers.loc[i, 'sell'])); b_net.append(int(buyers.loc[i, 'net'])); b_pct.append(f"{round((buyers.loc[i, 'net']/total_vol)*100)}%")
        else: b_n.append("-"); b_i.append(0); b_o.append(0); b_net.append(0); b_pct.append("-")
        if i < len(sellers):
            s_n.append(sellers.loc[i, 'securities_trader']); s_i.append(int(sellers.loc[i, 'buy'])); s_o.append(int(sellers.loc[i, 'sell'])); s_net.append(abs(int(sellers.loc[i, 'net']))); s_pct.append(f"{round((abs(sellers.loc[i, 'net'])/total_vol)*100)}%")
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
        html = safe_get_fubon(url) # 📌 使用安全降級爬蟲
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

def scrape_twse_block(latest_date):
    try:
        d_str = latest_date.replace("-", "")
        res = requests.get(f"https://www.twse.com.tw/rwd/zh/block/BFIAUU?date={d_str}&response=json", timeout=8).json()
        if "data" not in res or not res["data"]: return pd.DataFrame()
        fields = res.get("fields", [str(i) for i in range(len(res["data"][0]))])
        df = pd.DataFrame(res["data"], columns=fields)
        df = df[df.apply(lambda row: row.astype(str).str.contains(stock_id).any(), axis=1)]
        if not df.empty and '成交股數' in df.columns:
            df['成交張數'] = (pd.to_numeric(df['成交股數'].astype(str).str.replace(',',''), errors='coerce').fillna(0) / 1000).round().astype(int)
            df = df.drop(columns=['成交股數'])
        df.columns = list(df.columns)
        return df
    except: return pd.DataFrame()

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
    
    p_unit.columns.name = None; p_people.columns.name = None; p_pct.columns.name = None
    
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

def process_tdcc_dynamic(df_share, df_price, dead_chip_str, auto_dead_chip, base_money_str, influence_pct_str):
    if df_share.empty or df_price.empty: return pd.DataFrame()
    
    is_auto_chip = False
    if dead_chip_str and str(dead_chip_str).strip() != "":
        try: dead_chip_pct = float(str(dead_chip_str).replace('%', '').strip())
        except: dead_chip_pct = 0.0
    else:
        dead_chip_pct = auto_dead_chip
        is_auto_chip = True

    try: base_money_wan = float(str(base_money_str).replace(',', '').strip()) if base_money_str else 5000.0
    except: base_money_wan = 5000.0
    try: influence_rate = float(str(influence_pct_str).replace('%', '').strip()) / 100.0 if influence_pct_str else 0.005
    except: influence_rate = 0.005
    
    df_s = df_share.copy()
    df_p = df_price.copy()
    df_s['dt'] = pd.to_datetime(df_s['日期'])
    df_p['dt'] = pd.to_datetime(df_p['日期'])
    df_m = pd.merge_asof(df_s.sort_values('dt'), df_p.sort_values('dt')[['dt', '收盤價(元)']], on='dt', direction='backward').sort_values('dt', ascending=False)
    
    out = []
    for _, row in df_m.iterrows():
        p, d_str = row['收盤價(元)'], row['日期']
        if pd.isna(p) or p == 0: continue
        
        total_units = row.get('總張數', 0)
        money_t = (base_money_wan * 10000) / (p * 1000)
        infl_t = total_units * influence_rate
        raw_t = max(money_t, infl_t)
        ceiling_t = min([100, 200, 400, 600, 800, 1000], key=lambda x: 10000 if x < raw_t else x)
        
        l_cols = []
        if ceiling_t <= 100: l_cols = ['100-200張_比例(%)', '200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 200: l_cols = ['200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 400: l_cols = ['400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 600: l_cols = ['600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 800: l_cols = ['800-1000張_比例(%)', '1000張以上_比例(%)']
        else: l_cols = ['1000張以上_比例(%)']

        l_pct = sum([pd.to_numeric(row.get(c, 0), errors='coerce') for c in l_cols])
        
        c_display, status = "-", "未輸入死籌碼"
        if 0 < dead_chip_pct < 100:
            active_pool = 100.0 - dead_chip_pct
            c_val = (l_pct - dead_chip_pct) / active_pool
            c_val = max(0, c_val)
            status = "🔴 絕對控盤" if c_val >= 0.5 else "🟡 高度鎖碼" if c_val >= 0.3 else "🔵 初步集結" if c_val >= 0.15 else "⚪ 籌碼渙散"
            c_display = round(c_val * 100, 1)

        chip_label = f"{round(dead_chip_pct, 2)} ({'自動' if is_auto_chip else '手動'})" if dead_chip_pct > 0 else "-"

        out.append({
            "日期": d_str, "收盤價(元)": p, "股本(億)": round(total_units/10000, 2),
            "主導門檻": "影響力" if infl_t > money_t else "財力",
            "精算門檻(張)": ceiling_t, "級距總佔比(%)": round(l_pct, 2),
            "死籌碼(%)": chip_label,
            "活大戶影響力C(%)": c_display,
            "實戰判定": status
        })
    out_df = pd.DataFrame(out)
    if not out_df.empty: out_df.columns = list(out_df.columns)
    return out_df

# ==========================================
# 📌 1-2. V15.2 專家診斷引擎 (高敏防雷版)
# ==========================================
def get_expert_advice_v15_2(row, dead_shares):
    advice = []
    if pd.isna(row['1000張變動(%)']): return "⚪ 數據初始化..."
    
    leverage = 100 / (100 - dead_shares) if dead_shares < 100 and dead_shares > 0 else 1
    
    real_1000_change = row['1000張變動(%)'] * leverage
    real_combat_change = row['作戰區變動(%)'] * leverage

    max_intensity = real_1000_change if abs(real_1000_change) > abs(real_combat_change) else real_combat_change

    if row['總人數變動'] > 800 and (real_1000_change < -0.5 or real_combat_change < -0.5):
        advice.append(f"💀 [逃命警報] 散戶爆量接刀，活籌碼流出強度 {max_intensity:.1f}%")
        return " | ".join(advice)

    if real_1000_change > 3.0: 
        if row['總人數變動'] < 0:
            advice.append(f"🚀 [暴力軋空] 活大戶強力掃貨 {real_1000_change:.1f}% 且散戶退場")
        else:
            advice.append("⚠️ [異常集中] 提防大股東質押，需確認股價漲幅")

    if abs(row['1000張變動(%)']) <= 0.05 and real_combat_change > 0.8 and row['總人數變動'] < 0:
        advice.append(f"🔴 [降維鎖碼] 主力躲在中層暗中吃貨，吃貨強度 {real_combat_change:.1f}%")

    if row['中實戶人數變動'] == 0 and real_combat_change >= 0.5:
        advice.append("🔥 [定員增持] 原班人馬加碼，鎖碼意志極強！")

    if row['中實戶人數變動'] >= 2 and 200 <= row['K_Value'] <= 600:
        advice.append("🔴 [分身群聚] 偵測到隱藏合資集團，K值極度規律")

    if row['總人數變動'] > 800 and real_1000_change >= -0.2 and real_combat_change >= -0.2:
        advice.append("🟣 [惡意甩轎] 散戶湧入但主力未退，刻意讓道洗盤")

    return " | ".join(advice) if advice else "🔵 趨勢盤整/無明顯訊號"

def process_v15_ultimate_radar(df_wide, dead_chip_val, df_price):
    if df_wide.empty or len(df_wide) < 2: return pd.DataFrame()
    
    df = df_wide.sort_values('日期', ascending=True).copy()
    
    df['dt_end'] = pd.to_datetime(df['日期'])
    df_p = df_price.copy()
    if '日期' in df_p.columns and '收盤價(元)' in df_p.columns:
        df_p['dt'] = pd.to_datetime(df_p['日期'])
        df = pd.merge_asof(df.sort_values('dt_end'), df_p.sort_values('dt')[['dt', '收盤價(元)']], left_on='dt_end', right_on='dt', direction='backward')
    else:
        df['收盤價(元)'] = 0
    
    df['總股東人數'] = df['總人數(人)']
    df['1000張以上佔比(%)'] = df['1000張以上_比例(%)']
    
    df['中實戶人數'] = df['200-400張_人數']
    df['中實戶總數'] = df['200-400張_張數']
    
    df['核心區佔比(%)'] = df['400-600張_比例(%)'] + df['600-800張_比例(%)'] + df['800-1000張_比例(%)'] + df['1000張以上_比例(%)']
    df['作戰區佔比(%)'] = df['200-400張_比例(%)'] + df['400-600張_比例(%)'] + df['600-800張_比例(%)']

    df['總人數變動'] = df['總股東人數'].diff()
    df['總人數變動率(%)'] = (df['總股東人數'].diff() / df['總股東人數'].shift(1) * 100).round(2)
    df['1000張變動(%)'] = df['1000張以上佔比(%)'].diff().round(2)
    df['核心區變動(%)'] = df['核心區佔比(%)'].diff().round(2)
    df['作戰區變動(%)'] = df['作戰區佔比(%)'].diff().round(2)
    df['中實戶人數變動'] = df['中實戶人數'].diff()
    df['中實戶張數變動'] = df['中實戶總數'].diff()
    
    df['K_Value'] = np.where(df['中實戶人數變動'] > 0, (df['中實戶張數變動'] / df['中實戶人數變動']).round(1), 0.0)
    
    df['V15_實戰診斷'] = df.apply(lambda row: get_expert_advice_v15_2(row, dead_chip_val), axis=1)
    
    report_columns = ['日期', '收盤價(元)', '總人數變動率(%)', '1000張變動(%)', '作戰區變動(%)', 'K_Value', 'V15_實戰診斷']
    final_report = df[report_columns].sort_values('日期', ascending=False).fillna(0).head(10)
    final_report.columns = list(final_report.columns)
    return final_report

# ==========================================
# 其餘處理函式
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

def process_margin(df):
    if df.empty: return pd.DataFrame()
    cols = ["MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseCashRepayment", "MarginPurchaseTodayBalance", "ShortSaleBuy", "ShortSaleSell", "ShortSaleCashRepayment", "ShortSaleTodayBalance", "OffsetLoanAndShort", "MarginPurchaseYesterdayBalance", "ShortSaleYesterdayBalance"]
    for c in cols:
        if c in df.columns: df[c] = (pd.to_numeric(df[c], errors='coerce').fillna(0) / 1000).round().astype(int)
    df = df.rename(columns={"date":"日期","MarginPurchaseBuy":"融資買進(張)","MarginPurchaseSell":"融資賣出(張)","MarginPurchaseCashRepayment":"融資現償(張)","MarginPurchaseTodayBalance":"融資餘額(張)","ShortSaleBuy":"融券買進(張)","ShortSaleSell":"融券賣出(張)","ShortSaleTodayBalance":"融券餘額(張)","OffsetLoanAndShort":"資券相抵(張)"})
    df['融資增減(張)'] = df['融資餘額(張)'] - df['MarginPurchaseYesterdayBalance']
    df['融券增減(張)'] = df['融券餘額(張)'] - df['ShortSaleYesterdayBalance']
    df_res = df[['日期','融資買進(張)','融資賣出(張)','融資現償(張)','融資餘額(張)','融資增減(張)','融券買進(張)','融券賣出(張)','融券餘額(張)','融券增減(張)','資券相抵(張)']].tail(15).sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_inst(df):
    if df.empty: return pd.DataFrame()
    pdf = df.pivot_table(index='date', columns='name', values=['buy', 'sell'], fill_value=0).reset_index()
    pdf.columns = ['_'.join(c).strip('_') for c in pdf.columns.values]
    out = pd.DataFrame({'日期': pdf['date']})
    f_buy = pd.to_numeric(pdf.get('buy_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Foreign_Dealer_Self',0), errors='coerce').fillna(0)
    f_sell = pd.to_numeric(pdf.get('sell_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Foreign_Dealer_Self',0), errors='coerce').fillna(0)
    out['外資買賣超(張)'] = ((f_buy - f_sell) / 1000).round().astype(int)
    it_buy = pd.to_numeric(pdf.get('buy_Investment_Trust',0), errors='coerce').fillna(0); it_sell = pd.to_numeric(pdf.get('sell_Investment_Trust',0), errors='coerce').fillna(0)
    out['投信買賣超(張)'] = ((it_buy - it_sell) / 1000).round().astype(int)
    d_buy = pd.to_numeric(pdf.get('buy_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Dealer_Hedging',0), errors='coerce').fillna(0)
    d_sell = pd.to_numeric(pdf.get('sell_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Dealer_Hedging',0), errors='coerce').fillna(0)
    out['自營買賣超(張)'] = ((d_buy - d_sell) / 1000).round().astype(int)
    out['三大法人買賣超(張)'] = out['外資買賣超(張)'] + out['投信買賣超(張)'] + out['自營買賣超(張)']
    df_res = out.tail(15).sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_fut_inst(df):
    if df.empty: return pd.DataFrame()
    df['net'] = pd.to_numeric(df['long_open_interest_balance_volume'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_volume'], errors='coerce').fillna(0)
    pdf = df.pivot_table(index='date', columns='institutional_investors', values='net', fill_value=0).reset_index()
    pdf.columns.name = None
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    df_res = pdf.rename(columns={'date': '日期', 'Foreign_Investor': '外資多空(口)', 'Investment_Trust': '投信多空(口)', 'Dealer': '自營多空(口)'}).tail(15).sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_opt_inst(df):
    if df.empty: return pd.DataFrame()
    df['net_oi_amt'] = ((pd.to_numeric(df['long_open_interest_balance_amount'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_amount'], errors='coerce').fillna(0)) / 1000).round().astype(int)
    pdf = df.pivot_table(index=['date', 'call_put'], columns='institutional_investors', values='net_oi_amt', fill_value=0).reset_index()
    pdf.columns.name = None
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    pdf = pdf.rename(columns={'date': '日期', 'call_put': '契約', 'Foreign_Investor': '外資淨額(千元)', 'Investment_Trust': '投信淨額(千元)', 'Dealer': '自營商淨額(千元)'})
    pdf['契約'] = pdf['契約'].map({'Call': '買權(Call)', 'Put': '賣權(Put)'}).fillna(pdf['契約'])
    df_res = pdf[['日期', '契約', '外資淨額(千元)', '投信淨額(千元)', '自營商淨額(千元)']].tail(30).sort_values(['日期', '契約'], ascending=[False, True])
    df_res.columns = list(df_res.columns)
    return df_res

def process_per(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy().rename(columns={"date":"日期","dividend_yield":"殖利率(%)","PER":"本益比(倍)","PBR":"淨值比(倍)"})
    for col in ["殖利率(%)", "本益比(倍)", "淨值比(倍)"]: df_out[col] = pd.to_numeric(df_out[col], errors='coerce').round(2)
    df_res = df_out[['日期', '本益比(倍)', '淨值比(倍)', '殖利率(%)']].tail(15).sort_values('日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_disp(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy().rename(columns={"date":"公告日期","disposition_cnt":"處置次數","condition":"處置條件","measure":"處置措施","period_start":"處置起日","period_end":"處置迄日"})
    df_res = df_out[['公告日期', '處置次數', '處置起日', '處置迄日', '處置條件', '處置措施']].tail(5).sort_values('公告日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_div(df):
    if df.empty: return pd.DataFrame()
    df_out = df.rename(columns={"date": "公告日期", "year": "股利年份", "StockEarningsDistribution": "盈餘配股(元)", "StockStatutorySurplus": "公積配股(元)", "CashEarningsDistribution": "盈餘配息(元)", "CashStatutorySurplus": "公積配息(元)"})
    cols = [c for c in ["公告日期", "股利年份", "盈餘配息(元)", "公積配息(元)", "盈餘配股(元)", "公積配股(元)"] if c in df_out.columns]
    df_res = df_out[cols].tail(10).sort_values('公告日期', ascending=False)
    df_res.columns = list(df_res.columns)
    return df_res

def process_cbas(df):
    if df.empty: return pd.DataFrame()
    df_out = df.rename(columns={"date": "日期", "cb_id": "可轉債代號", "cb_name": "可轉債名稱", "ConversionPrice": "轉換價(元)", "PriceOfUnderlyingStock": "標的股價(元)", "OutstandingAmount": "未償還餘額", "CouponRate": "票面利率(%)"})
    cols = [c for c in ["日期", "可轉債代號", "可轉債名稱", "轉換價(元)", "標的股價(元)", "未償還餘額", "票面利率(%)"] if c in df_out.columns]
    df_res = df_out[cols]
    df_res.columns = list(df_res.columns)
    return df_res

# ==========================================
# 執行主引擎
# ==========================================
if run_btn:
    with st.spinner(f"正在擷取 {stock_id} 數據，並啟動 V15.2 破壁雷達..."):
        start_probe = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y-%m-%d")
        df_p_raw = fetch_fm("TaiwanStockPrice", start_probe)
        if df_p_raw.empty: st.error("查無股價資料"); st.stop()
        
        actual_dates = sorted(df_p_raw['date'].unique().tolist(), reverse=True)
        d_60 = actual_dates[59] if len(actual_dates) >= 60 else actual_dates[-1]
        df_price = process_price(df_p_raw)
        
        # 📌 執行死籌碼多重爬蟲引擎
        auto_dead_chip, chip_engine, debug_log = scrape_director_holding(stock_id)
        
        final_dead_chip = 0.0
        if dead_chip_input and str(dead_chip_input).strip() != "":
            try: final_dead_chip = float(str(dead_chip_input).replace('%', '').strip())
            except: final_dead_chip = auto_dead_chip
            st.toast("🤖 死籌碼：使用手動輸入數值", icon="✅")
        elif auto_dead_chip > 0:
            final_dead_chip = auto_dead_chip
            st.toast(f"🤖 死籌碼引擎：成功由 [{chip_engine}] 抓取到 {auto_dead_chip}%", icon="🎯")
        else:
            st.error(f"⚠️ 死籌碼引擎全滅！連線紀錄: {', '.join(debug_log)}。請於上方手動輸入死籌碼！")

        df_branch_raw = fetch_fm_branch_fast_parallel(actual_dates[:60])
        df_branch_diff = process_branch_diff(df_branch_raw, actual_dates)
        
        df_share_raw = fetch_fm("TaiwanStockHoldingSharesPer", d_60)
        df_share_wide, df_share_unit, df_share_people, df_share_pct, df_share_avg = process_tdcc(df_share_raw)
        
        df_share_dynamic = process_tdcc_dynamic(df_share_wide, df_price, dead_chip_input, auto_dead_chip, money_input, influence_input)
        
        # 📌 啟動 V15.2 專家診斷雷達
        df_v15_radar = process_v15_ultimate_radar(df_share_wide, final_dead_chip, df_price)
        
        df_twse = scrape_twse_block(actual_dates[0])
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

        st.success("✅ V15.2 引擎運算完畢！無懼 SSL 阻擋，富邦法人去重精算法已實裝。")
        
        def show(title, df, custom_class=""):
            st.markdown(f"#### {title}")
            if df is None or df.empty: st.warning("此區塊查無數據或無發行紀錄")
            else: 
                class_str = f"dataframe {custom_class}".strip()
                st.markdown(df.to_html(classes=class_str, index=False, border=1), unsafe_allow_html=True)
            
        show("▼▼▼ 1-1. 雙軸活大戶鎖碼判定表 (C-Value) ▼▼▼", df_share_dynamic)
        show("▼▼▼ 1-2. V15.2 專家診斷雷達 (富邦精算版) ▼▼▼", df_v15_radar, custom_class="radar-table")
        show("▼▼▼ 2-1. 集保分級 - 張數表 (近10週) ▼▼▼", df_share_unit)
        show("▼▼▼ 2-2. 集保分級 - 人數表 (近10週) ▼▼▼", df_share_people)
        show("▼▼▼ 2-3. 集保分級 - 比例表 (%) ▼▼▼", df_share_pct)
        show("▼▼▼ 2-4. 集保分級 - 均張表 ▼▼▼", df_share_avg)
        show("▼▼▼ 3. 鉅額交易明細 [來源：證交所] ▼▼▼", df_twse)
        show("▼▼▼ 4. 散戶資券餘額 [來源：FinMind] ▼▼▼", df_margin)
        show("▼▼▼ 5. 現股當沖明細 [來源：FinMind] ▼▼▼", df_day_trade)
        show("▼▼▼ 6. 法人買賣超 [來源：FinMind] ▼▼▼", df_inst)
        show("▼▼▼ 7. 收盤價量 [來源：FinMind] ▼▼▼", df_price.head(15))
        show("▼▼▼ 8. 月營收 (百萬元) - 近2年 [來源：FinMind] ▼▼▼", df_rev)
        
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
        p = f"請幫我分析 {stock_id} 的量化籌碼。已套用 V15.2 專家雷達，大戶門檻與活籌碼槓桿已雙軸精算。\n\n"
        p += format_to_gas(df_share_dynamic, "1-1. 雙軸活大戶鎖碼判定表 (C-Value)")
        p += format_to_gas(df_v15_radar, "1-2. V15.2 專家診斷雷達 (富邦精算版)")
        p += format_to_gas(df_share_unit, "2-1. 集保分級 - 張數表")
        p += format_to_gas(df_share_people, "2-2. 集保分級 - 人數表")
        p += format_to_gas(df_share_pct, "2-3. 集保分級 - 比例表 (%)")
        p += format_to_gas(df_share_avg, "2-4. 集保分級 - 均張表")
        p += format_to_gas(df_twse, "3. 鉅額交易明細")
        p += format_to_gas(df_margin, "4. 散戶資券餘額")
        p += format_to_gas(df_day_trade, "5. 現股當沖明細")
        p += format_to_gas(df_inst, "6. 法人買賣超")
        p += format_to_gas(df_price.head(15), "7. 收盤價量")
        p += format_to_gas(df_rev, "8. 月營收 (百萬元) - 近2年")
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
