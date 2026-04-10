import streamlit as st
import requests
import pandas as pd
import datetime
from io import StringIO
import time
import re
import concurrent.futures

# 設定網頁標題與佈局
st.set_page_config(page_title="台股全息量化系統 (活大戶鎖碼版)", layout="wide")

# 內建最新 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0xMCAyMDoyMDo0NiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.7s3-IrkfdiUyTvGiZQGESBUBAPHQTnd4pwYcn8_J-CY"

st.title("🤖 交易員實戰手冊：全息量化擷取系統")
st.markdown("✅ **活大戶影響力 C-Value 引擎** | ✅ **100/400/1000 動態門檻** | ✅ **多線程極速並發**")

# UI 輸入區
col1, col2, col3, col4 = st.columns([1, 1, 1, 2])
with col1:
    stock_id = st.text_input("輸入個股代號", value="7711")
with col2:
    bs_diff = st.text_input("買賣家數差 (選填)", placeholder="-150")
with col3:
    dead_chip_input = st.text_input("死籌碼 % (選填)", placeholder="例如: 57")
with col4:
    st.write(""); st.write("")
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
# 核心大戶加總邏輯優化 (V29 活大戶鎖碼版)
# ==========================================
def process_tdcc(df):
    if df.empty: return df
    df = df[~df['HoldingSharesLevel'].astype(str).str.contains('差異數')]
    l_map = {"1":"1-999股", "2":"1-5張", "3":"5-10張", "4":"10-15張", "5":"15-20張", "6":"20-30張", "7":"30-40張", "8":"40-50張", "9":"50-100張", "10":"100-200張", "11":"200-400張", "12":"400-600張", "13":"600-800張", "14":"800-1000張", "15":"1000張以上"}
    df['LevelClean'] = df['HoldingSharesLevel'].astype(str).str.strip().map(l_map).fillna(df['HoldingSharesLevel'])
    df['people'] = pd.to_numeric(df['people'], errors='coerce').fillna(0).astype(int)
    df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0)
    df['unit'] = (pd.to_numeric(df.get('unit', 0), errors='coerce').fillna(0) / 1000).round().astype(int)
    
    dates = sorted(df['date'].unique(), reverse=True)[:5]
    df = df[df['date'].isin(dates)]
    df_levels = df[~df['LevelClean'].str.contains('合計|總計')]
    df_total = df_levels.groupby('date')[['people', 'unit']].sum().reset_index().rename(columns={'people': '總人數(人)', 'unit': '總張數'})
    df_pivot = df_levels.pivot(index='date', columns='LevelClean', values=['people', 'unit', 'percent']).reset_index()
    new_cols = ['date']
    for c in df_pivot.columns[1:]:
        m_name = {'people': '人數', 'unit': '張數', 'percent': '比例(%)'}.get(c[0], c[0])
        new_cols.append(f"{c[1]}_{m_name}")
    df_pivot.columns = new_cols
    df_out = pd.merge(df_total, df_pivot, on='date', how='left').rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    return df_out

def process_tdcc_dynamic(df_share, df_price, df_sh, dead_chip_str):
    """【V29】活大戶 C-Value 實戰引擎"""
    if df_share.empty or df_price.empty: return pd.DataFrame()
    
    # 處理死籌碼輸入
    try: dead_chip_pct = float(dead_chip_str) / 100.0 if dead_chip_str else 0.0
    except: dead_chip_pct = 0.0
    
    official_shares = pd.to_numeric(df_sh.iloc[0].get('NumberOfSharesIssued', 0), errors='coerce') if not df_sh.empty else 0
    df_share['dt'] = pd.to_datetime(df_share['日期']); df_price['dt'] = pd.to_datetime(df_price['日期'])
    df_m = pd.merge_asof(df_share.sort_values('dt'), df_price.sort_values('dt')[['dt', '收盤價(元)']], on='dt', direction='backward').sort_values('dt', ascending=False)
    
    out = []
    for _, row in df_m.iterrows():
        p = row['收盤價(元)']
        if pd.isna(p) or p == 0: continue
        total_units = row.get('總張數', 0)
        
        cap_b = official_shares / 10000000 if official_shares > 0 else total_units / 10000
        
        # 1. 實戰三步判定：定門檻
        if p > 500 or cap_b < 10: closest_t = 100
        elif p >= 100 or cap_b <= 50: closest_t = 400
        else: closest_t = 1000
        
        # 2. 動態加總大戶佔比
        large_pct = 0
        for col in [c for c in row.index if '比例(%)' in str(c)]:
            nums = re.findall(r'\d+', str(col).replace(',', ''))
            if nums and int(nums[0]) >= closest_t: 
                large_pct += row[col]
        
        # 3. 活大戶影響力 C-Value 判定
        if dead_chip_pct > 0 and dead_chip_pct < 1:
            active_pool = 1.0 - dead_chip_pct
            c_val = ((large_pct / 100.0) - dead_chip_pct) / active_pool
            c_val = max(0, c_val) # 防極端值
            
            if c_val >= 0.5: status = "🔴 絕對控盤"
            elif c_val >= 0.3: status = "🟡 高度影響"
            elif c_val < 0.15: status = "⚪ 散戶主導"
            else: status = "🔵 盤整觀察"
        else:
            c_val = large_pct / 100.0
            status = "未輸入死籌碼"

        out.append({
            "日期": row['日期'], 
            "收盤價": p, 
            "股本(億)": round(cap_b, 2),
            "大戶門檻(張)": closest_t, 
            "級距總佔比(%)": int(round(large_pct)),
            "死籌碼(%)": int(dead_chip_pct * 100) if dead_chip_str else "-",
            "活大戶影響力C(%)": round(c_val * 100, 1) if dead_chip_str else "-",
            "實戰判定": status
        })
    return pd.DataFrame(out)

# ==========================================
# 其餘處理函式
# ==========================================
def process_price(df):
    df_out = df.copy()
    df_out['Trading_Volume'] = (pd.to_numeric(df_out['Trading_Volume'], errors='coerce').fillna(0) / 1000).round().astype(int)
    df_out = df_out.rename(columns={"date":"日期","Trading_Volume":"成交量(張)","Trading_money":"成交金額(千元)","open":"開盤價(元)","max":"最高價(元)","min":"最低價(元)","close":"收盤價(元)","spread":"漲跌(元)"})
    df_out["斷頭價(0.78)"] = (df_out["收盤價(元)"] * 0.78).round(2)
    return df_out[['日期','成交量(張)','開盤價(元)','最高價(元)','最低價(元)','收盤價(元)','漲跌(元)','斷頭價(0.78)']].tail(15).sort_values('日期', ascending=False)

def process_margin(df):
    cols = ["MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseCashRepayment", "MarginPurchaseTodayBalance", "ShortSaleBuy", "ShortSaleSell", "ShortSaleCashRepayment", "ShortSaleTodayBalance", "OffsetLoanAndShort", "MarginPurchaseYesterdayBalance", "ShortSaleYesterdayBalance"]
    for c in cols:
        if c in df.columns: df[c] = (pd.to_numeric(df[c], errors='coerce').fillna(0) / 1000).round().astype(int)
    df = df.rename(columns={"date":"日期","MarginPurchaseBuy":"融資買進(張)","MarginPurchaseSell":"融資賣出(張)","MarginPurchaseCashRepayment":"融資現償(張)","MarginPurchaseTodayBalance":"融資餘額(張)","ShortSaleBuy":"融券買進(張)","ShortSaleSell":"融券賣出(張)","ShortSaleTodayBalance":"融券餘額(張)","OffsetLoanAndShort":"資券相抵(張)"})
    df['融資增減(張)'] = df['融資餘額(張)'] - df['MarginPurchaseYesterdayBalance']
    df['融券增減(張)'] = df['融券餘額(張)'] - df['ShortSaleYesterdayBalance']
    return df[['日期','融資買進(張)','融資賣出(張)','融資現償(張)','融資餘額(張)','融資增減(張)','融券買進(張)','融券賣出(張)','融券餘額(張)','融券增減(張)','資券相抵(張)']].tail(15).sort_values('日期', ascending=False)

def process_inst(df):
    pdf = df.pivot_table(index='date', columns='name', values=['buy', 'sell'], fill_value=0).reset_index()
    pdf.columns = ['_'.join(c).strip('_') for c in pdf.columns.values]
    out = pd.DataFrame({'日期': pdf['date']})
    f_net = (pd.to_numeric(pdf.get('buy_Foreign_Investor',0),0)+pd.to_numeric(pdf.get('buy_Foreign_Dealer_Self',0),0)) - (pd.to_numeric(pdf.get('sell_Foreign_Investor',0),0)+pd.to_numeric(pdf.get('sell_Foreign_Dealer_Self',0),0))
    out['外資買賣超(張)'] = (f_net / 1000).round().astype(int)
    it_net = pd.to_numeric(pdf.get('buy_Investment_Trust',0),0) - pd.to_numeric(pdf.get('sell_Investment_Trust',0),0)
    out['投信買賣超(張)'] = (it_net / 1000).round().astype(int)
    d_net = (pd.to_numeric(pdf.get('buy_Dealer_self',0),0)+pd.to_numeric(pdf.get('buy_Dealer_Hedging',0),0)) - (pd.to_numeric(pdf.get('sell_Dealer_self',0),0)+pd.to_numeric(pdf.get('sell_Dealer_Hedging',0),0))
    out['自營買賣超(張)'] = (d_net / 1000).round().astype(int)
    out['三大法人買賣超(張)'] = out['外資買賣超(張)'] + out['投信買賣超(張)'] + out['自營買賣超(張)']
    return out.tail(15).sort_values('日期', ascending=False)

def process_fut_inst(df):
    df['net'] = pd.to_numeric(df['long_open_interest_balance_volume'],0) - pd.to_numeric(df['short_open_interest_balance_volume'],0)
    pdf = df.pivot_table(index='date', columns='institutional_investors', values='net', fill_value=0).reset_index()
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    return pdf.rename(columns={'date': '日期', 'Foreign_Investor': '外資多空(口)', 'Investment_Trust': '投信多空(口)', 'Dealer': '自營多空(口)'}).tail(15).sort_values('日期', ascending=False)

if run_btn:
    with st.spinner(f"正在擷取 {stock_id} 數據，並計算活大戶 C-Value..."):
        start_probe = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y-%m-%d")
        df_price_raw = fetch_fm("TaiwanStockPrice", start_probe)
        if df_price_raw.empty: st.error("查無股價資料"); st.stop()
        
        d_60 = sorted(df_price_raw['date'].unique(), reverse=True)[59] if len(df_price_raw['date'].unique()) > 60 else df_price_raw['date'].min()
        
        df_sh = fetch_fm("TaiwanStockShareholding", d_60)
        df_share_raw = fetch_fm("TaiwanStockHoldingSharesPer", d_60)
        df_share_expanded = process_tdcc(df_share_raw)
        df_price = process_price(df_price_raw)
        
        # 帶入死籌碼輸入值
        df_share_dynamic = process_tdcc_dynamic(df_share_expanded, df_price, df_sh, dead_chip_input)
        
        df_twse = scrape_twse_block(df_price_raw['date'].max())
        df_margin = process_margin(fetch_fm("TaiwanStockMarginPurchaseShortSale", d_60))
        df_inst = process_inst(fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", d_60))
        df_rev_raw = fetch_fm("TaiwanStockMonthRevenue", "2024-01-01")
        df_rev = pd.DataFrame()
        if not df_rev_raw.empty:
            df_rev_raw['營收月份'] = df_rev_raw['revenue_year'].astype(str) + "-" + df_rev_raw['revenue_month'].astype(str).str.zfill(2)
            df_rev = df_rev_raw.rename(columns={"revenue":"月營收(百萬元)"})[['營收月份','月營收(百萬元)']].tail(15) if '營收月份' in df_rev_raw.columns else pd.DataFrame()
            if not df_rev.empty: df_rev['月營收(百萬元)'] = (df_rev['月營收(百萬元)']/1000000).round().astype(int)
        
        df_branch_raw = fetch_fm_branch_fast_parallel(sorted(df_price_raw['date'].unique(), reverse=True)[:60])
        df_b_today = process_branch_data(df_branch_raw, 1, sorted(df_price_raw['date'].unique(), reverse=True))
        df_pledge_summary, df_pledge_detail = scrape_fubon_pledge(df_price_raw)
        df_fut = process_fut_inst(fetch_fm("TaiwanFuturesInstitutionalInvestors", d_60, specific_id=False, target_id="TX"))

        st.success("✅ C-Value 引擎運算完畢！活大戶影響力已更新。")
        def show(title, df):
            st.markdown(f"#### {title}")
            if df is None or df.empty: st.warning("此區塊查無數據")
            else: st.markdown(df.to_html(index=False, border=1), unsafe_allow_html=True)
            
        show("▼▼▼ 1. 活大戶鎖碼 C-Value 判定表 ▼▼▼", df_share_dynamic)
        show("▼▼▼ 2. 詳細集保分級展開明細 [來源：FinMind] ▼▼▼", df_share_expanded)
        show("▼▼▼ 3. 鉅額交易明細 [來源：證交所] ▼▼▼", df_twse)
        show("▼▼▼ 4. 散戶資券餘額 [來源：FinMind] ▼▼▼", df_margin)
        show("▼▼▼ 5. 法人買賣超 [來源：FinMind] ▼▼▼", df_inst)
        show("▼▼▼ 6. 收盤價量 [來源：FinMind] ▼▼▼", df_price)
        show("▼▼▼ 7. 月營收 (百萬元) [來源：FinMind] ▼▼▼", df_rev)
        show(f"▼▼▼ 8. 主力分點 - 今日 [來源：FinMind] ▼▼▼", df_b_today)
        st.markdown("#### ▼▼▼ 9. 董監大股東質設明細 [來源：富邦證券] ▼▼▼")
        if not df_pledge_summary.empty: st.markdown(df_pledge_summary.to_html(index=False, border=1), unsafe_allow_html=True)
        show("▼▼▼ 10. 台指期貨三大法人未平倉 (大盤) ▼▼▼", df_fut)

        st.divider(); st.subheader("📋 【給 Gemini 的量化分析資料包】")
        p = f"請幫我分析 {stock_id} 的量化籌碼。已使用 C-Value 過濾死籌碼，找出真實活大戶影響力。\n\n"
        p += format_to_gas(df_share_dynamic, "1. 活大戶鎖碼判定表 (C-Value)")
        p += format_to_gas(df_share_expanded, "2. 集保詳細分級")
        p += format_to_gas(df_margin, "3. 散戶資券餘額")
        p += format_to_gas(df_inst, "4. 法人買賣超")
        p += format_to_gas(df_price, "5. 收盤價量")
        p += format_to_gas(df_b_today, "6. 今日主力分點")
        p += format_pledge_to_gas(df_pledge_summary, df_pledge_detail)
        p += format_to_gas(df_fut, "7. 大盤期貨籌碼")
        st.code(p, language="text")
