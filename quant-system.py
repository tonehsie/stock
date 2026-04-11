import streamlit as st
import requests
import pandas as pd
import datetime
from io import StringIO
import time
import re
import concurrent.futures

# 設定網頁標題與佈局
st.set_page_config(page_title="台股全息量化系統 (雙軸大戶鎖碼版)", layout="wide")

# 內建最新 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0xMCAyMDoyMDo0NiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.7s3-IrkfdiUyTvGiZQGESBUBAPHQTnd4pwYcn8_J-CY"

st.title("🤖 交易員實戰手冊：全息量化擷取系統")
st.markdown("✅ **橫向表格加總(絕不重複)** | ✅ **雙軸大戶 C-Value 引擎** | ✅ **集保四維拆解**")

# UI 輸入區 (加入自訂雙軸參數)
col1, col2, col3, col4, col5 = st.columns([1, 1, 1, 1, 1])
with col1:
    stock_id = st.text_input("個股代號", value="7711")
with col2:
    bs_diff = st.text_input("買賣家數差", placeholder="選填 (如 -150)")
with col3:
    dead_chip_input = st.text_input("死籌碼 %", placeholder="選填 (如 57)")
with col4:
    money_input = st.text_input("財力設定(萬)", value="5000")
with col5:
    influence_input = st.text_input("影響力設定(%)", value="0.5")

st.write("")
run_btn = st.button("🚀 啟動引擎：擷取全息資料並產生 Prompt", use_container_width=True)

st.divider()

# ==========================================
# 工具函式
# ==========================================
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
    if df is None or df.empty:
        return header + "此區塊查無最新數據或無發行紀錄, \n"
    csv_str = df.to_csv(index=False)
    lines = [line.replace('"', '') + ", " for line in csv_str.strip().split('\n')]
    return header + "\n".join(lines) + "\n"

def format_pledge_to_gas(df_summary, df_detail):
    if df_summary is None or df_summary.empty:
        return "▼▼▼ 9. 董監大股東質設 ▼▼▼, \n此區塊查無最新數據或無發行紀錄, \n"
    return format_to_gas(df_summary, "9. 董監大股東質設 (餘額與斷頭預警)") + format_to_gas(df_detail, "董監大股東質設 (異動明細)")

# ==========================================
# 資料清洗與爬蟲引擎
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
    headers = {"User-Agent": "Mozilla/5.0"}
    for i in range(3):
        url = f"https://fubon-ebrokerdj.fbs.com.tw/z/zc/zc0/zc06_{stock_id}_{i}.djhtm"
        try:
            res = requests.get(url, headers=headers, timeout=10)
            res.encoding = 'big5'
            p = extract_fubon_table(res.text, "設質人身", 7)
            if p: all_data.extend(p)
        except: pass
    if not all_data: return pd.DataFrame(), pd.DataFrame()
    seen = set()
    uniq_data = []
    for r in all_data:
        key = "|".join(r)
        if key not in seen:
            seen.add(key)
            uniq_data.append(r)
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
    return pd.DataFrame(summary_rows), df_all

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
        return df
    except: return pd.DataFrame()

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
            completed += 1; status_text.text(f"📥 並發下載分點資料中... ({completed}/{len(dates_list)})"); progress_bar.progress(completed / len(dates_list))
            if f.result(): all_data.extend(f.result())
    status_text.empty(); progress_bar.empty()
    return pd.DataFrame(all_data)

def process_branch_data(df_raw, period_days, actual_dates):
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
    max_len = min(15, max(len(buyers), len(sellers)))
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
    return out

# ==========================================
# 核心大戶加總邏輯優化 (V38 橫向完美精算版)
# ==========================================
def clean_level_by_math(x):
    """📌 暴力數學解析器"""
    s = str(x).replace(',', '').replace(' ', '')
    if s in ["17", "17.0", "合計", "總計"]: return "合計"
    
    nums = re.findall(r'\d+', s)
    if not nums: return s
    
    if len(nums) == 1 and int(nums[0]) <= 15:
        mapping = {1: "1-999股", 2: "1-5張", 3: "5-10張", 4: "10-15張", 5: "15-20張", 6: "20-30張", 7: "30-40張", 8: "40-50張", 9: "50-100張", 10: "100-200張", 11: "200-400張", 12: "400-600張", 13: "600-800張", 14: "800-1000張", 15: "1000張以上"}
        return mapping.get(int(nums[0]), s)
        
    upper_bound = int(nums[-1])
    
    if upper_bound <= 999: return "1-999股"
    elif upper_bound <= 5000: return "1-5張"
    elif upper_bound <= 10000: return "5-10張"
    elif upper_bound <= 15000: return "10-15張"
    elif upper_bound <= 20000: return "15-20張"
    elif upper_bound <= 30000: return "20-30張"
    elif upper_bound <= 40000: return "30-40張"
    elif upper_bound <= 50000: return "40-50張"
    elif upper_bound <= 100000: return "50-100張"
    elif upper_bound <= 200000: return "100-200張"
    elif upper_bound <= 400000: return "200-400張"
    elif upper_bound <= 600000: return "400-600張"
    elif upper_bound <= 800000: return "600-800張"
    elif upper_bound <= 1000000: return "800-1000張"
    else: return "1000張以上"

def process_tdcc(df):
    """📌 橫向精算邏輯：攤平表格後再相加，防禦任何重複資料"""
    if df.empty: return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df = df[~df['HoldingSharesLevel'].astype(str).str.contains('差異數')]
    df['LevelClean'] = df['HoldingSharesLevel'].apply(clean_level_by_math)
    
    df['people'] = pd.to_numeric(df['people'], errors='coerce').fillna(0).astype(int)
    df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0)
    df['unit'] = (pd.to_numeric(df.get('unit', 0), errors='coerce').fillna(0) / 1000).round().astype(int)
    
    dates = sorted(df['date'].unique(), reverse=True)[:5]
    df = df[df['date'].isin(dates)]
    
    # 剃除官方總計，避免污染
    df_levels = df[~df['LevelClean'].str.contains('合計|總計')]
    
    # 先做 Pivot，確保一天只有一行，每個級距只有一格 (遇到重複會自動取平均或覆蓋，防禦重疊髒資料)
    df_pivot_unit = df_levels.pivot_table(index='date', columns='LevelClean', values='unit', aggfunc='first').reset_index().fillna(0)
    df_pivot_people = df_levels.pivot_table(index='date', columns='LevelClean', values='people', aggfunc='first').reset_index().fillna(0)
    df_pivot_percent = df_levels.pivot_table(index='date', columns='LevelClean', values='percent', aggfunc='first').reset_index().fillna(0)
    
    level_order = ['1-999股', '1-5張', '5-10張', '10-15張', '15-20張', '20-30張', '30-40張', '40-50張', '50-100張', '100-200張', '200-400張', '400-600張', '600-800張', '800-1000張', '1000張以上']
    
    # 確保 15 個欄位都存在
    for col in level_order:
        if col not in df_pivot_unit.columns: df_pivot_unit[col] = 0
        if col not in df_pivot_people.columns: df_pivot_people[col] = 0
        if col not in df_pivot_percent.columns: df_pivot_percent[col] = 0

    # 📌 終極防呆：從攤平後的表格「橫向加總」，絕對不會重複計算
    df_total = pd.DataFrame({'date': df_pivot_unit['date']})
    df_total['總張數'] = df_pivot_unit[level_order].sum(axis=1)
    df_total['總人數(人)'] = df_pivot_people[level_order].sum(axis=1)
    df_total['總均張'] = df_total.apply(lambda row: round(row['總張數'] / row['總人數(人)'], 2) if row['總人數(人)'] > 0 else 0, axis=1)
    
    # 1. 產生供 C-Value 計算的大寬表
    df_wide = df_total.copy()
    for col in level_order:
        df_wide[f"{col}_張數"] = df_pivot_unit[col]
        df_wide[f"{col}_人數"] = df_pivot_people[col]
        df_wide[f"{col}_比例(%)"] = df_pivot_percent[col]
    df_wide = df_wide.rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    # 2. 產生 UI 顯示用的 4 張獨立子表
    cols_ordered = ['date'] + level_order
    
    df_unit = pd.merge(df_total[['date', '總張數']], df_pivot_unit[cols_ordered], on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    df_people = pd.merge(df_total[['date', '總人數(人)']], df_pivot_people[cols_ordered], on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    df_percent = df_pivot_percent[cols_ordered].rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    # 均張表
    df_avg_base = pd.DataFrame({'date': df_pivot_unit['date']})
    for col in level_order:
        df_avg_base[col] = (df_pivot_unit[col] / df_pivot_people[col].replace(0, pd.NA)).fillna(0).round(2)
    df_avg = pd.merge(df_total[['date', '總均張']], df_avg_base, on='date').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    
    return df_wide, df_unit, df_people, df_percent, df_avg

def process_tdcc_dynamic(df_share, df_price, dead_chip_str, base_money_str, influence_pct_str):
    if df_share.empty or df_price.empty: return pd.DataFrame()
    
    try: dead_chip_pct = float(str(dead_chip_str).replace('%', '').strip()) if dead_chip_str else 0.0
    except: dead_chip_pct = 0.0
    try: base_money_wan = float(str(base_money_str).replace(',', '').strip()) if base_money_str else 5000.0
    except: base_money_wan = 5000.0
    try: influence_rate = float(str(influence_pct_str).replace('%', '').strip()) / 100.0 if influence_pct_str else 0.005
    except: influence_rate = 0.005
    
    df_share['dt'] = pd.to_datetime(df_share['日期']); df_price['dt'] = pd.to_datetime(df_price['日期'])
    df_m = pd.merge_asof(df_share.sort_values('dt'), df_price.sort_values('dt')[['dt', '收盤價(元)']], on='dt', direction='backward').sort_values('dt', ascending=False)
    
    out = []
    for _, row in df_m.iterrows():
        p = row['收盤價(元)']
        if pd.isna(p) or p == 0: continue
        
        total_units = row.get('總張數', 0)
        cap_b = total_units / 10000 
        
        money_threshold = (base_money_wan * 10000) / (p * 1000)
        influence_threshold = total_units * influence_rate
        raw_t = max(money_threshold, influence_threshold)
        
        valid_thresholds = [100, 200, 400, 600, 800, 1000]
        ceiling_t = 1000
        for t in valid_thresholds:
            if t >= raw_t:
                ceiling_t = t
                break
        
        large_cols = []
        if ceiling_t <= 100: large_cols = ['100-200張_比例(%)', '200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 200: large_cols = ['200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 400: large_cols = ['400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 600: large_cols = ['600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 800: large_cols = ['800-1000張_比例(%)', '1000張以上_比例(%)']
        else: large_cols = ['1000張以上_比例(%)']

        large_pct = 0.0
        for c in large_cols:
            val = row.get(c, 0)
            if not pd.isna(val):
                try: large_pct += float(val)
                except: pass
        
        if dead_chip_pct > 0 and dead_chip_pct < 100:
            active_pool = 100.0 - dead_chip_pct
            c_val = (large_pct - dead_chip_pct) / active_pool
            c_val = max(0, c_val) 
            
            if c_val > 0.5: status = "🔴 絕對控盤"
            elif c_val >= 0.3: status = "🟡 高度鎖碼"
            elif c_val >= 0.15: status = "🔵 初步集結"
            else: status = "⚪ 籌碼渙散"
            
            c_display = round(c_val * 100, 1)
        else:
            c_val = large_pct / 100.0
            status = "未輸入死籌碼"
            c_display = "-"

        out.append({
            "日期": row['日期'], 
            "收盤價": p, 
            "股本(億)": round(cap_b, 2),
            "主導門檻": "影響力" if influence_threshold > money_threshold else "財力",
            "精算門檻(張)": ceiling_t, 
            "級距總佔比(%)": round(large_pct, 2),
            "死籌碼(%)": round(dead_chip_pct, 2) if dead_chip_str else "-",
            "活大戶影響力C(%)": c_display,
            "實戰判定": status
        })
    return pd.DataFrame(out)

# ==========================================
# 其餘處理函式
# ==========================================
def process_price(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    df_out['Trading_Volume'] = (pd.to_numeric(df_out['Trading_Volume'], errors='coerce').fillna(0) / 1000).round().astype(int)
    df_out = df_out.rename(columns={"date":"日期","Trading_Volume":"成交量(張)","Trading_money":"成交金額(千元)","open":"開盤價(元)","max":"最高價(元)","min":"最低價(元)","close":"收盤價(元)","spread":"漲跌(元)"})
    df_out["斷頭價(0.78)"] = (df_out["收盤價(元)"] * 0.78).round(2)
    return df_out[['日期','成交量(張)','開盤價(元)','最高價(元)','最低價(元)','收盤價(元)','漲跌(元)','斷頭價(0.78)']].tail(15).sort_values('日期', ascending=False)

def process_margin(df):
    if df.empty: return pd.DataFrame()
    cols = ["MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseCashRepayment", "MarginPurchaseTodayBalance", "ShortSaleBuy", "ShortSaleSell", "ShortSaleCashRepayment", "ShortSaleTodayBalance", "OffsetLoanAndShort", "MarginPurchaseYesterdayBalance", "ShortSaleYesterdayBalance"]
    for c in cols:
        if c in df.columns: df[c] = (pd.to_numeric(df[c], errors='coerce').fillna(0) / 1000).round().astype(int)
    df = df.rename(columns={"date":"日期","MarginPurchaseBuy":"融資買進(張)","MarginPurchaseSell":"融資賣出(張)","MarginPurchaseCashRepayment":"融資現償(張)","MarginPurchaseTodayBalance":"融資餘額(張)","ShortSaleBuy":"融券買進(張)","ShortSaleSell":"融券賣出(張)","ShortSaleTodayBalance":"融券餘額(張)","OffsetLoanAndShort":"資券相抵(張)"})
    df['融資增減(張)'] = df['融資餘額(張)'] - df['MarginPurchaseYesterdayBalance']
    df['融券增減(張)'] = df['融券餘額(張)'] - df['ShortSaleYesterdayBalance']
    return df[['日期','融資買進(張)','融資賣出(張)','融資現償(張)','融資餘額(張)','融資增減(張)','融券買進(張)','融券賣出(張)','融券餘額(張)','融券增減(張)','資券相抵(張)']].tail(15).sort_values('日期', ascending=False)

def process_inst(df):
    if df.empty: return pd.DataFrame()
    pdf = df.pivot_table(index='date', columns='name', values=['buy', 'sell'], fill_value=0).reset_index()
    pdf.columns = ['_'.join(c).strip('_') for c in pdf.columns.values]
    out = pd.DataFrame({'日期': pdf['date']})
    
    f_buy = pd.to_numeric(pdf.get('buy_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Foreign_Dealer_Self',0), errors='coerce').fillna(0)
    f_sell = pd.to_numeric(pdf.get('sell_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Foreign_Dealer_Self',0), errors='coerce').fillna(0)
    out['外資買賣超(張)'] = ((f_buy - f_sell) / 1000).round().astype(int)
    
    it_buy = pd.to_numeric(pdf.get('buy_Investment_Trust',0), errors='coerce').fillna(0)
    it_sell = pd.to_numeric(pdf.get('sell_Investment_Trust',0), errors='coerce').fillna(0)
    out['投信買賣超(張)'] = ((it_buy - it_sell) / 1000).round().astype(int)
    
    d_buy = pd.to_numeric(pdf.get('buy_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Dealer_Hedging',0), errors='coerce').fillna(0)
    d_sell = pd.to_numeric(pdf.get('sell_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Dealer_Hedging',0), errors='coerce').fillna(0)
    out['自營買賣超(張)'] = ((d_buy - d_sell) / 1000).round().astype(int)
    
    out['三大法人買賣超(張)'] = out['外資買賣超(張)'] + out['投信買賣超(張)'] + out['自營買賣超(張)']
    return out.tail(15).sort_values('日期', ascending=False)

def process_fut_inst(df):
    if df.empty: return pd.DataFrame()
    df['net'] = pd.to_numeric(df['long_open_interest_balance_volume'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_volume'], errors='coerce').fillna(0)
    pdf = df.pivot_table(index='date', columns='institutional_investors', values='net', fill_value=0).reset_index()
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    return pdf.rename(columns={'date': '日期', 'Foreign_Investor': '外資多空(口)', 'Investment_Trust': '投信多空(口)', 'Dealer': '自營多空(口)'}).tail(15).sort_values('日期', ascending=False)

def process_opt_inst(df):
    if df.empty: return pd.DataFrame()
    df['net_oi_amt'] = ((pd.to_numeric(df['long_open_interest_balance_amount'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_amount'], errors='coerce').fillna(0)) / 1000).round().astype(int)
    pdf = df.pivot_table(index=['date', 'call_put'], columns='institutional_investors', values='net_oi_amt', fill_value=0).reset_index()
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    pdf = pdf.rename(columns={'date': '日期', 'call_put': '契約', 'Foreign_Investor': '外資淨額(千元)', 'Investment_Trust': '投信淨額(千元)', 'Dealer': '自營商淨額(千元)'})
    pdf['契約'] = pdf['契約'].map({'Call': '買權(Call)', 'Put': '賣權(Put)'}).fillna(pdf['契約'])
    return pdf[['日期', '契約', '外資淨額(千元)', '投信淨額(千元)', '自營商淨額(千元)']].tail(30).sort_values(['日期', '契約'], ascending=[False, True])

def process_per(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    df_out = df_out.rename(columns={"date":"日期","dividend_yield":"殖利率(%)","PER":"本益比(倍)","PBR":"淨值比(倍)"})
    for col in ["殖利率(%)", "本益比(倍)", "淨值比(倍)"]:
        df_out[col] = pd.to_numeric(df_out[col], errors='coerce').round(2)
    return df_out[['日期', '本益比(倍)', '淨值比(倍)', '殖利率(%)']].tail(15).sort_values('日期', ascending=False)

def process_disp(df):
    if df.empty: return pd.DataFrame()
    df_out = df.copy()
    df_out = df_out.rename(columns={"date":"公告日期","disposition_cnt":"處置次數","condition":"處置條件","measure":"處置措施","period_start":"處置起日","period_end":"處置迄日"})
    return df_out[['公告日期', '處置次數', '處置起日', '處置迄日', '處置條件', '處置措施']].tail(5).sort_values('公告日期', ascending=False)

def process_div(df):
    if df.empty: return pd.DataFrame()
    rename_map = {"date": "公告日期", "year": "股利年份", "StockEarningsDistribution": "盈餘配股(元)", "StockStatutorySurplus": "公積配股(元)", "CashEarningsDistribution": "盈餘配息(元)", "CashStatutorySurplus": "公積配息(元)"}
    df_out = df.rename(columns=rename_map)
    cols = [c for c in ["公告日期", "股利年份", "盈餘配息(元)", "公積配息(元)", "盈餘配股(元)", "公積配股(元)"] if c in df_out.columns]
    return df_out[cols].tail(10).sort_values('公告日期', ascending=False)

def process_cbas(df):
    if df.empty: return pd.DataFrame()
    rename_map = {"date": "日期", "cb_id": "可轉債代號", "cb_name": "可轉債名稱", "ConversionPrice": "轉換價(元)", "PriceOfUnderlyingStock": "標的股價(元)", "OutstandingAmount": "未償還餘額", "CouponRate": "票面利率(%)"}
    df_out = df.rename(columns=rename_map)
    cols = [c for c in ["日期", "可轉債代號", "可轉債名稱", "轉換價(元)", "標的股價(元)", "未償還餘額", "票面利率(%)"] if c in df_out.columns]
    return df_out[cols]

# ==========================================
# 執行主引擎
# ==========================================
if run_btn:
    with st.spinner(f"正在擷取 {stock_id} 數據，並進行列轉行後聚合精算..."):
        start_probe = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y-%m-%d")
        df_price_raw = fetch_fm("TaiwanStockPrice", start_probe)
        if df_price_raw.empty: st.error("查無股價資料"); st.stop()
        
        actual_dates = sorted(df_price_raw['date'].unique().tolist(), reverse=True)
        d_60 = actual_dates[59] if len(actual_dates) >= 60 else actual_dates[-1]
        
        df_share_raw = fetch_fm("TaiwanStockHoldingSharesPer", d_60)
        df_share_wide, df_share_unit, df_share_people, df_share_pct, df_share_avg = process_tdcc(df_share_raw)
        
        df_price = process_price(df_price_raw)
        df_share_dynamic = process_tdcc_dynamic(df_share_wide, df_price, dead_chip_input, money_input, influence_input)
        
        df_twse = scrape_twse_block(actual_dates[0])
        df_margin = process_margin(fetch_fm("TaiwanStockMarginPurchaseShortSale", d_60))
        df_inst = process_inst(fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", d_60))
        df_rev_raw = fetch_fm("TaiwanStockMonthRevenue", "2024-01-01")
        df_rev = pd.DataFrame()
        if not df_rev_raw.empty:
            df_rev_raw['營收月份'] = df_rev_raw['revenue_year'].astype(str) + "-" + df_rev_raw['revenue_month'].astype(str).str.zfill(2)
            df_rev = df_rev_raw.rename(columns={"revenue":"月營收(百萬元)"})[['營收月份','月營收(百萬元)']].tail(15) if '營收月份' in df_rev_raw.columns else pd.DataFrame()
            if not df_rev.empty: df_rev['月營收(百萬元)'] = (df_rev['月營收(百萬元)']/1000000).round().astype(int)
        
        df_branch_raw = fetch_fm_branch_fast_parallel(actual_dates[:60])
        df_b_today = process_branch_data(df_branch_raw, 1, actual_dates)
        df_b_prev1 = process_branch_data(df_branch_raw, 1, actual_dates[1:]) if len(actual_dates) > 1 else pd.DataFrame()
        df_b_3 = process_branch_data(df_branch_raw, 3, actual_dates)
        df_b_10 = process_branch_data(df_branch_raw, 10, actual_dates)
        df_b_20 = process_branch_data(df_branch_raw, 20, actual_dates)
        df_b_30 = process_branch_data(df_branch_raw, 30, actual_dates)
        df_b_60 = process_branch_data(df_branch_raw, 60, actual_dates)

        df_gov = pd.DataFrame()
        if not df_b_today.empty:
            govs = ["台銀", "土銀", "彰銀", "第一", "兆豐", "華南", "合庫", "台企銀"]
            df_gov = df_b_today[df_b_today.astype(str).apply(lambda x: x.str.contains('|'.join(govs))).any(axis=1)]

        df_pledge_summary, df_pledge_detail = scrape_fubon_pledge(df_price_raw)
        df_fut = process_fut_inst(fetch_fm("TaiwanFuturesInstitutionalInvestors", d_60, specific_id=False, target_id="TX"))
        
        df_div = process_div(fetch_fm("TaiwanStockDividend", "2015-01-01"))
        df_per = process_per(fetch_fm("TaiwanStockPER", d_60))
        start_probe_180 = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y-%m-%d")
        df_disp = process_disp(fetch_fm("TaiwanStockDispositionSecuritiesPeriod", start_probe_180))
        
        df_cbas_raw = fetch_fm("TaiwanStockConvertibleBondDailyOverview", actual_dates[0], specific_id=False)
        df_cbas_filtered = df_cbas_raw[df_cbas_raw['cb_id'].astype(str).str.startswith(stock_id)] if not df_cbas_raw.empty else pd.DataFrame()
        df_cbas = process_cbas(df_cbas_filtered)
        
        df_opt_inst = process_opt_inst(fetch_fm("TaiwanOptionInstitutionalInvestors", d_60, specific_id=False, target_id="TXO"))

        st.success("✅ V38 引擎運算完畢！總計數值已完美對齊橫向 15 級距。")
        def show(title, df):
            st.markdown(f"#### {title}")
            if df is None or df.empty: st.warning("此區塊查無數據或無發行紀錄")
            else: st.markdown(df.to_html(index=False, border=1), unsafe_allow_html=True)
            
        show("▼▼▼ 1. 雙軸活大戶鎖碼判定表 (C-Value) ▼▼▼", df_share_dynamic)
        show("▼▼▼ 2-1. 集保分級 - 張數表 ▼▼▼", df_share_unit)
        show("▼▼▼ 2-2. 集保分級 - 人數表 ▼▼▼", df_share_people)
        show("▼▼▼ 2-3. 集保分級 - 比例表 (%) ▼▼▼", df_share_pct)
        show("▼▼▼ 2-4. 集保分級 - 均張表 ▼▼▼", df_share_avg)
        show("▼▼▼ 3. 鉅額交易明細 [來源：證交所] ▼▼▼", df_twse)
        show("▼▼▼ 4. 散戶資券餘額 [來源：FinMind] ▼▼▼", df_margin)
        show("▼▼▼ 5. 法人買賣超 [來源：FinMind] ▼▼▼", df_inst)
        show("▼▼▼ 6. 收盤價量 [來源：FinMind] ▼▼▼", df_price)
        show("▼▼▼ 7. 月營收 (百萬元) [來源：FinMind] ▼▼▼", df_rev)
        show(f"▼▼▼ 8. 主力分點 - 今日 ({actual_dates[0]}) [來源：FinMind] ▼▼▼", df_b_today)
        
        st.markdown("#### ▼▼▼ 9. 董監大股東質設明細 [來源：富邦證券] ▼▼▼")
        if df_pledge_detail.empty: st.warning("此區塊查無數據或無發行紀錄")
        else:
            if not df_pledge_summary.empty: st.markdown(df_pledge_summary.to_html(index=False, border=1), unsafe_allow_html=True)
            st.markdown(df_pledge_detail.to_html(index=False, border=1), unsafe_allow_html=True)
            
        show("▼▼▼ 10. 台指期貨三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_fut)
        show("▼▼▼ 11. 歷年股利 [來源：FinMind] ▼▼▼", df_div)
        show("▼▼▼ 12. 本益比、淨值比與殖利率 [來源：FinMind] ▼▼▼", df_per)
        show("▼▼▼ 13. 處置有價證券狀態 [來源：FinMind] ▼▼▼", df_disp)
        show(f"▼▼▼ 14. 主力分點 - 前一日 ({actual_dates[1] if len(actual_dates)>1 else '無'}) [來源：FinMind] ▼▼▼", df_b_prev1)
        show("▼▼▼ 15. 主力分點 - 近3日 [來源：FinMind] ▼▼▼", df_b_3)
        show("▼▼▼ 16. 主力分點 - 近10日 [來源：FinMind] ▼▼▼", df_b_10)
        show("▼▼▼ 17. 主力分點 - 近20日 [來源：FinMind] ▼▼▼", df_b_20)
        show("▼▼▼ 18. 主力分點 - 近30日 [來源：FinMind] ▼▼▼", df_b_30)
        show("▼▼▼ 19. 主力分點 - 近60日 [來源：FinMind] ▼▼▼", df_b_60)
        show("▼▼▼ 20. 八大官股進出 (今日) [來源：FinMind] ▼▼▼", df_gov)
        show("▼▼▼ 21. CBAS 可轉債數據 [來源：FinMind] ▼▼▼", df_cbas)
        show("▼▼▼ 22. 台指選擇權三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_opt_inst)
        
        st.markdown("#### ▼▼▼ 23. 買賣家數差明細 (手動) [來源：使用者輸入] ▼▼▼")
        if not bs_diff: st.warning("此區塊目前查無數據")
        else: st.info(f"使用者輸入數值：{bs_diff}")

        st.divider(); st.subheader("📋 【給 Gemini 的量化分析資料包】")
        p = f"請幫我分析 {stock_id} 的量化籌碼。大戶門檻已根據「財力與影響力」雙軸精算，並以 C-Value 過濾死籌碼。\n\n"
        p += format_to_gas(df_share_dynamic, "1. 雙軸活大戶鎖碼判定表 (C-Value)")
        p += format_to_gas(df_share_unit, "2-1. 集保分級 - 張數表")
        p += format_to_gas(df_share_people, "2-2. 集保分級 - 人數表")
        p += format_to_gas(df_share_pct, "2-3. 集保分級 - 比例表 (%)")
        p += format_to_gas(df_share_avg, "2-4. 集保分級 - 均張表")
        p += format_to_gas(df_twse, "3. 鉅額交易明細")
        p += format_to_gas(df_margin, "4. 散戶資券餘額")
        p += format_to_gas(df_inst, "5. 法人買賣超")
        p += format_to_gas(df_price, "6. 收盤價量")
        p += format_to_gas(df_rev, "7. 月營收 (百萬元)")
        p += format_to_gas(df_b_today, f"8. 主力分點 - 今日 ({actual_dates[0]})")
        p += format_pledge_to_gas(df_pledge_summary, df_pledge_detail)
        p += format_to_gas(df_fut, "10. 台指期貨三大法人未平倉 (大盤)")
        p += format_to_gas(df_div, "11. 歷年股利")
        p += format_to_gas(df_per, "12. 本益比、淨值比與殖利率")
        p += format_to_gas(df_disp, "13. 處置有價證券狀態")
        p += format_to_gas(df_b_prev1, "14. 主力分點 - 前一日")
        p += format_to_gas(df_b_3, "15. 主力分點 - 近3日")
        p += format_to_gas(df_b_10, "16. 主力分點 - 近10日")
        p += format_to_gas(df_b_20, "17. 主力分點 - 近20日")
        p += format_to_gas(df_b_30, "18. 主力分點 - 近30日")
        p += format_to_gas(df_b_60, "19. 主力分點 - 近60日")
        p += format_to_gas(df_gov, "20. 八大官股進出 (今日)")
        p += format_to_gas(df_cbas, "21. CBAS 可轉債數據")
        p += format_to_gas(df_opt_inst, "22. 台指選擇權三大法人未平倉 (大盤)")
        p += f"▼▼▼ 23. 買賣家數差明細 (手動) ▼▼▼, \n{bs_diff + ',' if bs_diff else '此區塊查無最新數據或無發行紀錄,'}\n"
        
        st.code(p, language="text")
