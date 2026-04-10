import streamlit as st
import requests
import pandas as pd
import datetime
from io import StringIO
import time
import re
import concurrent.futures

# 設定網頁標題與佈局
st.set_page_config(page_title="台股全息量化系統 (全數據霸王版)", layout="wide")

# 內建 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0wOSAxOToxMTo0MiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.32OOXXWwga3QGGh5SQe7JHw03wfFfQo4XDohfgSI0d8"

st.title("🤖 交易員實戰手冊：全息量化擷取系統")
st.markdown("✅ **營收真實月份校正** | ✅ **新增 PE/PB 估值與處置狀態** | ✅ **多線程極速並發**")

# UI 輸入區
col1, col2, col3 = st.columns([1, 1, 2])
with col1:
    stock_id = st.text_input("輸入個股代號", value="8027")
with col2:
    bs_diff = st.text_input("買賣家數差數值 (手動)", placeholder="-150")
with col3:
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
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    pledge_cur_y = current_year
    pledge_last_m = 99
    parsed_dates = []
    for d_str in df_all['日期']:
        if len(d_str) == 5 and '/' in d_str: 
            m = int(d_str.split('/')[0])
            if pledge_last_m == 99:
                if m > current_month + 1 and current_month < 3: pledge_cur_y = current_year - 1
                else: pledge_cur_y = current_year
            elif m > pledge_last_m + 1: pledge_cur_y -= 1
            pledge_last_m = m
            parsed_dates.append(f"{pledge_cur_y}-{d_str.replace('/', '-')}")
        elif len(d_str) >= 7 and '/' in d_str: 
            pts = d_str.split('/')
            y = int(pts[0]) + 1911
            pledge_cur_y = y
            pledge_last_m = int(pts[1])
            parsed_dates.append(f"{y}-{pts[1]}-{pts[2]}")
        else: parsed_dates.append(d_str)
            
    df_all['日期'] = parsed_dates
    for col in ["設質(張)", "解質(張)", "累積質設(張)"]:
        df_all[col] = pd.to_numeric(df_all[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
        
    price_dict = {}
    if not df_price_raw.empty:
        for _, row in df_price_raw.iterrows():
            try:
                pd_date = pd.to_datetime(row['date']).strftime('%Y-%m-%d')
                price_dict[pd_date] = row['close']
            except: pass
            
    pledge_prices = []
    margin_calls = []
    for _, row in df_all.iterrows():
        d_str = row['日期']
        sz = row['設質(張)']
        if sz > 0:
            found_price = "-"
            mc = "-"
            try:
                target_d = pd.to_datetime(d_str)
                for i in range(20):
                    check_d = (target_d - datetime.timedelta(days=i)).strftime('%Y-%m-%d')
                    if check_d in price_dict:
                        found_price = price_dict[check_d]
                        mc = round(found_price * 0.78, 2)
                        break
            except: pass
            pledge_prices.append(found_price)
            margin_calls.append(mc)
        else:
            pledge_prices.append("-")
            margin_calls.append("-")
            
    df_all['設質日收盤價'] = pledge_prices
    df_all['強制賣出價(0.78)'] = margin_calls
    
    summary_map = {}
    for _, r in df_all.iterrows():
        name = r['姓名']
        if name not in summary_map:
            summary_map[name] = {"title": r['身份別'], "balance": r['累積質設(張)'], "pledgePrice": "-", "marginCall": "-"}
        if summary_map[name]["pledgePrice"] == "-" and r['設質(張)'] > 0:
            summary_map[name]["pledgePrice"] = r['設質日收盤價']
            summary_map[name]["marginCall"] = r['強制賣出價(0.78)']

    summary_rows = []
    for name, data in summary_map.items():
        if data["balance"] > 0:
            summary_rows.append({
                "身份別": data["title"],
                "姓名": name,
                "目前剩餘質設(張)": data["balance"],
                "最後設質收盤價(元)": data["pledgePrice"],
                "估算斷頭價(0.78)": data["marginCall"]
            })
            
    df_summary = pd.DataFrame(summary_rows)
    return df_summary, df_all

def scrape_twse_block(latest_date):
    try:
        d_str = latest_date.replace("-", "")
        data = requests.get(f"https://www.twse.com.tw/rwd/zh/block/BFIAUU?date={d_str}&response=json", timeout=8).json()
        df = pd.DataFrame(data.get("data", []))
        df = df[df.apply(lambda row: row.astype(str).str.contains(stock_id).any(), axis=1)]
        if not df.empty and df.shape[1] >= 10:
            df.iloc[:, 4] = (pd.to_numeric(df.iloc[:, 4].astype(str).str.replace(',',''), errors='coerce').fillna(0) / 1000).round().astype(int)
        return df
    except: return pd.DataFrame()

def fetch_single_day_branch(d):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": "TaiwanStockTradingDailyReport", "data_id": stock_id, "start_date": d, "end_date": d}
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=15).json()
        if "data" in res and res["data"]: return res["data"]
    except: pass
    return []

def fetch_fm_branch_fast_parallel(dates_list):
    all_data = []
    progress_bar = st.progress(0)
    status_text = st.empty()
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_date = {executor.submit(fetch_single_day_branch, d): d for d in dates_list}
        completed = 0
        for future in concurrent.futures.as_completed(future_to_date):
            completed += 1
            status_text.text(f"📥 多線程極速並發下載中... ({completed}/{len(dates_list)})")
            progress_bar.progress(completed / len(dates_list))
            data = future.result()
            if data: all_data.extend(data)
    status_text.empty()
    progress_bar.empty()
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
# 各項資料處理函式
# ==========================================
def process_tdcc(df):
    if df.empty: return df
    df = df[~df['HoldingSharesLevel'].astype(str).str.contains('差異數')]
    level_map = {"1":"1-999股", "1.0":"1-999股", "2":"1-5張", "2.0":"1-5張", "3":"5-10張", "3.0":"5-10張", "4":"10-15張", "4.0":"10-15張", "5":"15-20張", "5.0":"15-20張", "6":"20-30張", "6.0":"20-30張", "7":"30-40張", "7.0":"30-40張", "8":"40-50張", "8.0":"40-50張", "9":"50-100張", "9.0":"50-100張", "10":"100-200張", "10.0":"100-200張", "11":"200-400張", "11.0":"200-400張", "12":"400-600張", "12.0":"400-600張", "13":"600-800張", "13.0":"600-800張", "14":"800-1000張", "14.0":"800-1000張", "15":"1000張以上", "15.0":"1000張以上", "17":"合計", "17.0":"合計"}
    df['HoldingSharesLevel'] = df['HoldingSharesLevel'].astype(str).map(level_map).fillna(df['HoldingSharesLevel'])
    df['people'] = pd.to_numeric(df['people'], errors='coerce').fillna(0).astype(int)
    df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0).round().astype(int)
    if 'unit' in df.columns: df['unit'] = (pd.to_numeric(df['unit'], errors='coerce').fillna(0) / 1000).round().astype(int)
    else: df['unit'] = 0
    latest_dates = sorted(df['date'].unique(), reverse=True)[:5]
    df = df[df['date'].isin(latest_dates)]
    df_total = df[df['HoldingSharesLevel'] == '合計'][['date', 'people', 'unit']].rename(columns={'people': '總人數(人)', 'unit': '總張數'})
    df_levels = df[df['HoldingSharesLevel'] != '合計']
    df_pivot = df_levels.pivot(index='date', columns='HoldingSharesLevel', values=['people', 'unit', 'percent']).reset_index()
    new_cols = []
    for c in df_pivot.columns:
        if c[0] == 'date' or c == 'date': new_cols.append('date')
        else: new_cols.append(f"{c[1]}_{{'people': '人數', 'unit': '張數', 'percent': '比例(%)'}.get(c[0], c[0])}")
    df_pivot.columns = new_cols
    df_out = pd.merge(df_total, df_pivot, on='date', how='left') if not df_total.empty else df_pivot
    if df_total.empty: df_out['總人數(人)'] = 0; df_out['總張數'] = 0
    df_out = df_out.rename(columns={'date': '日期'}).sort_values('日期', ascending=False)
    level_order = ['1-999股', '1-5張', '5-10張', '10-15張', '15-20張', '20-30張', '30-40張', '40-50張', '50-100張', '100-200張', '200-400張', '400-600張', '600-800張', '800-1000張', '1000張以上']
    ordered_cols = ['日期', '總人數(人)', '總張數'] + [f"{lvl}_{m}" for lvl in level_order for m in ['人數', '張數', '比例(%)']]
    final_cols = [c for c in ordered_cols if c in df_out.columns]
    for c in df_out.columns:
        if c not in final_cols: final_cols.append(c)
    return df_out[final_cols]

def process_margin(df):
    if df.empty: return df
    cols_to_fix = ["MarginPurchaseBuy", "MarginPurchaseSell", "MarginPurchaseCashRepayment", "MarginPurchaseTodayBalance", "ShortSaleBuy", "ShortSaleSell", "ShortSaleCashRepayment", "ShortSaleTodayBalance", "OffsetLoanAndShort", "MarginPurchaseYesterdayBalance", "ShortSaleYesterdayBalance"]
    for c in cols_to_fix:
        if c in df.columns: df[c] = (pd.to_numeric(df[c], errors='coerce').fillna(0) / 1000).round().astype(int)
    df = df.rename(columns={"date":"日期","MarginPurchaseBuy":"融資買進(張)","MarginPurchaseSell":"融資賣出(張)","MarginPurchaseCashRepayment":"融資現償(張)","MarginPurchaseTodayBalance":"融資餘額(張)","ShortSaleBuy":"融券買進(張)","ShortSaleSell":"融券賣出(張)","ShortSaleTodayBalance":"融券餘額(張)","OffsetLoanAndShort":"資券相抵(張)"})
    df['融資增減(張)'] = df['融資餘額(張)'] - df['MarginPurchaseYesterdayBalance']
    df['融券增減(張)'] = df['融券餘額(張)'] - df['ShortSaleYesterdayBalance']
    return df[['日期','融資買進(張)','融資賣出(張)','融資現償(張)','融資餘額(張)','融資增減(張)','融券買進(張)','融券賣出(張)','融券餘額(張)','融券增減(張)','資券相抵(張)']].tail(15).sort_values('日期', ascending=False)

def process_inst(df):
    if df.empty: return df
    pdf = df.pivot_table(index='date', columns='name', values=['buy', 'sell'], fill_value=0).reset_index()
    pdf.columns = ['_'.join(c).strip('_') for c in pdf.columns.values]
    out = pd.DataFrame({'日期': pdf['date']})
    f_buy = (pd.to_numeric(pdf.get('buy_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Foreign_Dealer_Self',0), errors='coerce').fillna(0)) / 1000
    f_sell = (pd.to_numeric(pdf.get('sell_Foreign_Investor',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Foreign_Dealer_Self',0), errors='coerce').fillna(0)) / 1000
    out['外資買賣超(張)'] = (f_buy - f_sell).round().astype(int)
    it_buy = pd.to_numeric(pdf.get('buy_Investment_Trust',0), errors='coerce').fillna(0) / 1000
    it_sell = pd.to_numeric(pdf.get('sell_Investment_Trust',0), errors='coerce').fillna(0) / 1000
    out['投信買賣超(張)'] = (it_buy - it_sell).round().astype(int)
    d_buy = (pd.to_numeric(pdf.get('buy_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('buy_Dealer_Hedging',0), errors='coerce').fillna(0)) / 1000
    d_sell = (pd.to_numeric(pdf.get('sell_Dealer_self',0), errors='coerce').fillna(0) + pd.to_numeric(pdf.get('sell_Dealer_Hedging',0), errors='coerce').fillna(0)) / 1000
    out['自營買賣超(張)'] = (d_buy - d_sell).round().astype(int)
    out['三大法人買賣超(張)'] = out['外資買賣超(張)'] + out['投信買賣超(張)'] + out['自營買賣超(張)']
    return out.tail(15).sort_values('日期', ascending=False)

def process_price(df):
    if df.empty: return df
    df_out = df.copy()
    df_out['Trading_Volume'] = (pd.to_numeric(df_out['Trading_Volume'], errors='coerce').fillna(0) / 1000).round().astype(int)
    df_out['Trading_money'] = pd.to_numeric(df_out['Trading_money'], errors='coerce').fillna(0).round().astype(int)
    df_out = df_out.rename(columns={"date":"日期","Trading_Volume":"成交量(張)","Trading_money":"成交金額(千元)","open":"開盤價(元)","max":"最高價(元)","min":"最低價(元)","close":"收盤價(元)","spread":"漲跌(元)"})
    df_out["斷頭價(0.78)"] = (df_out["收盤價(元)"] * 0.78).round(2)
    return df_out[['日期','成交量(張)','開盤價(元)','最高價(元)','最低價(元)','收盤價(元)','漲跌(元)','斷頭價(0.78)']].tail(15).sort_values('日期', ascending=False)

def process_fut_inst(df):
    if df.empty: return df
    df['net_oi'] = pd.to_numeric(df['long_open_interest_balance_volume'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_volume'], errors='coerce').fillna(0)
    pdf = df.pivot_table(index='date', columns='institutional_investors', values='net_oi', fill_value=0).reset_index()
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    pdf = pdf.rename(columns={'date': '日期', 'Foreign_Investor': '外資多空淨額(口)', 'Investment_Trust': '投信多空淨額(口)', 'Dealer': '自營商多空淨額(口)'})
    return pdf[['日期', '外資多空淨額(口)', '投信多空淨額(口)', '自營商多空淨額(口)']].tail(15).sort_values('日期', ascending=False)

def process_opt_inst(df):
    if df.empty: return df
    df['net_oi_amt'] = ((pd.to_numeric(df['long_open_interest_balance_amount'], errors='coerce').fillna(0) - pd.to_numeric(df['short_open_interest_balance_amount'], errors='coerce').fillna(0)) / 1000).round().astype(int)
    pdf = df.pivot_table(index=['date', 'call_put'], columns='institutional_investors', values='net_oi_amt', fill_value=0).reset_index()
    for col in ['Foreign_Investor', 'Investment_Trust', 'Dealer']:
        if col not in pdf.columns: pdf[col] = 0
    pdf = pdf.rename(columns={'date': '日期', 'call_put': '契約', 'Foreign_Investor': '外資淨額(千元)', 'Investment_Trust': '投信淨額(千元)', 'Dealer': '自營商淨額(千元)'})
    pdf['契約'] = pdf['契約'].map({'Call': '買權(Call)', 'Put': '賣權(Put)'}).fillna(pdf['契約'])
    return pdf[['日期', '契約', '外資淨額(千元)', '投信淨額(千元)', '自營商淨額(千元)']].tail(30).sort_values(['日期', '契約'], ascending=[False, True])

def process_per(df):
    if df.empty: return df
    df_out = df.copy()
    df_out = df_out.rename(columns={"date":"日期","dividend_yield":"殖利率(%)","PER":"本益比(倍)","PBR":"淨值比(倍)"})
    for col in ["殖利率(%)", "本益比(倍)", "淨值比(倍)"]:
        df_out[col] = pd.to_numeric(df_out[col], errors='coerce').round(2)
    return df_out[['日期', '本益比(倍)', '淨值比(倍)', '殖利率(%)']].tail(15).sort_values('日期', ascending=False)

def process_disp(df):
    if df.empty: return df
    df_out = df.copy()
    df_out = df_out.rename(columns={"date":"公告日期","disposition_cnt":"處置次數","condition":"處置條件","measure":"處置措施","period_start":"處置起日","period_end":"處置迄日"})
    return df_out[['公告日期', '處置次數', '處置起日', '處置迄日', '處置條件', '處置措施']].tail(5).sort_values('公告日期', ascending=False)

def format_pledge_to_gas(df_summary, df_detail):
    header = "▼▼▼ 20. 董監大股東質設明細 [來源：富邦證券] ▼▼▼, \n"
    if df_detail is None or df_detail.empty: return header + "本檔股票近 3 年內無董監事或大股東質設紀錄, \n"
    res = header
    if not df_summary.empty:
        res += "【目前質設餘額與斷頭預警】, \n" + df_summary.to_csv(index=False).replace('"', '') + "\n"
    res += "【近3年質設異動明細】, \n" + df_detail.to_csv(index=False).replace('"', '')
    return res + "\n"

# ==========================================
# 執行主引擎
# ==========================================
if run_btn:
    with st.spinner(f"正在擷取 {stock_id} 全息量化數據與大盤期權籌碼..."):
        start_probe_3y = (datetime.date.today() - datetime.timedelta(days=1095)).strftime("%Y-%m-%d")
        df_price_raw = fetch_fm("TaiwanStockPrice", start_probe_3y)
        
        if df_price_raw.empty:
            st.error(f"無法取得 {stock_id} 價格資料。")
        else:
            actual_dates = sorted(df_price_raw['date'].unique().tolist(), reverse=True)
            d_latest = actual_dates[0]
            d_60 = actual_dates[59] if len(actual_dates) >= 60 else actual_dates[-1]

            # 基礎個股資料
            df_share = process_tdcc(fetch_fm("TaiwanStockHoldingSharesPer", d_60))
            df_twse = scrape_twse_block(d_latest)
            df_margin = process_margin(fetch_fm("TaiwanStockMarginPurchaseShortSale", d_60))
            
            df_sbl_raw = fetch_fm("TaiwanDailyShortSaleBalances", d_60)
            df_sbl = pd.DataFrame()
            if not df_sbl_raw.empty:
                df_sbl = df_sbl_raw.copy()
                df_sbl['SBLShortSalesCurrentDayBalance'] = (pd.to_numeric(df_sbl['SBLShortSalesCurrentDayBalance'], errors='coerce').fillna(0) / 1000).round().astype(int)
                df_sbl = df_sbl.rename(columns={"date":"日期","SBLShortSalesCurrentDayBalance":"借券餘額(張)"})[['日期','借券餘額(張)']].tail(15).sort_values('日期', ascending=False)
            
            df_daytrade_raw = fetch_fm("TaiwanStockDayTrading", d_60)
            df_daytrade = pd.DataFrame()
            if not df_daytrade_raw.empty:
                df_daytrade = df_daytrade_raw.copy()
                df_daytrade['Volume'] = (pd.to_numeric(df_daytrade['Volume'], errors='coerce').fillna(0) / 1000).round().astype(int)
                df_daytrade['BuyAmount'] = pd.to_numeric(df_daytrade['BuyAmount'], errors='coerce').fillna(0).astype(int)
                df_daytrade['SellAmount'] = pd.to_numeric(df_daytrade['SellAmount'], errors='coerce').fillna(0).astype(int)
                df_daytrade = df_daytrade.rename(columns={"date":"日期","Volume":"當沖數量(張)","BuyAmount":"買進金額","SellAmount":"賣出金額"}).tail(15).sort_values('日期', ascending=False)
            
            df_inst = process_inst(fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", d_60))
            df_price = process_price(df_price_raw)
            
            df_rev_raw = fetch_fm("TaiwanStockMonthRevenue", "2024-01-01")
            df_rev = pd.DataFrame()
            if not df_rev_raw.empty:
                df_rev = df_rev_raw.copy()
                # 關鍵修復：利用 revenue_year 和 revenue_month 組合出正確的真實營收月份
                df_rev['營收月份'] = df_rev['revenue_year'].astype(str) + "-" + df_rev['revenue_month'].astype(str).str.zfill(2)
                df_rev['revenue'] = (pd.to_numeric(df_rev['revenue'], errors='coerce').fillna(0) / 1000000).round().astype(int)
                df_rev = df_rev.rename(columns={"revenue":"月營收(百萬元)"})[['營收月份','月營收(百萬元)']].tail(15)
            
            df_div = fetch_fm("TaiwanStockDividend", "2015-01-01")
            if not df_div.empty: df_div = df_div.tail(10)
            
            df_per = process_per(fetch_fm("TaiwanStockPER", d_60))
            start_probe_180 = (datetime.date.today() - datetime.timedelta(days=180)).strftime("%Y-%m-%d")
            df_disp = process_disp(fetch_fm("TaiwanStockDispositionSecuritiesPeriod", start_probe_180))

            # 分點資料
            dates_to_fetch = actual_dates[:60]
            df_branch_raw = fetch_fm_branch_fast_parallel(dates_to_fetch)
            df_b_today = process_branch_data(df_branch_raw, 1, actual_dates)
            df_b_prev1 = process_branch_data(df_branch_raw, 1, actual_dates[1:]) if len(actual_dates) > 1 else pd.DataFrame()
            df_b_3 = process_branch_data(df_branch_raw, 3, actual_dates)
            df_b_10 = process_branch_data(df_branch_raw, 10, actual_dates)
            df_b_20 = process_branch_data(df_branch_raw, 20, actual_dates)
            df_b_30 = process_branch_data(df_branch_raw, 30, actual_dates)
            df_b_60 = process_branch_data(df_branch_raw, 60, actual_dates)
            
            df_19 = pd.DataFrame()
            if not df_b_today.empty:
                govs = ["台銀", "土銀", "彰銀", "第一", "兆豐", "華南", "合庫", "台企銀"]
                df_19 = df_b_today[df_b_today.astype(str).apply(lambda x: x.str.contains('|'.join(govs))).any(axis=1)]

            df_pledge_summary, df_pledge_detail = scrape_fubon_pledge(df_price_raw)
            df_cbas_raw = fetch_fm("TaiwanStockConvertibleBondDailyOverview", d_latest, specific_id=False)
            df_cbas = df_cbas_raw[df_cbas_raw['cb_id'].astype(str).str.startswith(stock_id)] if not df_cbas_raw.empty else pd.DataFrame()

            df_fut_inst = process_fut_inst(fetch_fm("TaiwanFuturesInstitutionalInvestors", d_60, specific_id=False, target_id="TX"))
            df_opt_inst = process_opt_inst(fetch_fm("TaiwanOptionInstitutionalInvestors", d_60, specific_id=False, target_id="TXO"))

            # UI 顯示
            st.success(f"✅ 營收月份精準校正完成！")
            def render_html_table(title, df):
                st.markdown(f"#### {title}")
                if df is None or df.empty: st.warning("此區塊目前查無數據")
                else: st.markdown(df.to_html(index=False, border=1), unsafe_allow_html=True)
            
            render_html_table("▼▼▼ 1. 最新集保分級明細 [來源：FinMind] ▼▼▼", df_share)
            render_html_table("▼▼▼ 2. 鉅額交易明細 [來源：證交所] ▼▼▼", df_twse)
            render_html_table("▼▼▼ 3. 散戶資券餘額 [來源：FinMind] ▼▼▼", df_margin)
            render_html_table("▼▼▼ 4. 借券賣出與餘額 [來源：FinMind] ▼▼▼", df_sbl)
            render_html_table("▼▼▼ 5. 當沖明細 [來源：FinMind] ▼▼▼", df_daytrade)
            render_html_table("▼▼▼ 6. 法人買賣超 [來源：FinMind] ▼▼▼", df_inst)
            render_html_table("▼▼▼ 7. 收盤價量 [來源：FinMind] ▼▼▼", df_price)
            render_html_table("▼▼▼ 8. 月營收 [來源：FinMind] ▼▼▼", df_rev)
            render_html_table("▼▼▼ 9. 歷年股利 [來源：FinMind] ▼▼▼", df_div)
            render_html_table("▼▼▼ 10. 本益比、淨值比與殖利率 [來源：FinMind] ▼▼▼", df_per)
            render_html_table("▼▼▼ 11. 處置有價證券狀態 [來源：FinMind] ▼▼▼", df_disp)
            render_html_table(f"▼▼▼ 12. 主力分點 - 今日 ({actual_dates[0]}) [來源：FinMind] ▼▼▼", df_b_today)
            render_html_table(f"▼▼▼ 13. 主力分點 - 前一日 ({actual_dates[1] if len(actual_dates)>1 else '無'}) [來源：FinMind] ▼▼▼", df_b_prev1)
            render_html_table("▼▼▼ 14. 主力分點 - 近3日 [來源：FinMind] ▼▼▼", df_b_3)
            render_html_table("▼▼▼ 15. 主力分點 - 近10日 [來源：FinMind] ▼▼▼", df_b_10)
            render_html_table("▼▼▼ 16. 主力分點 - 近20日 [來源：FinMind] ▼▼▼", df_b_20)
            render_html_table("▼▼▼ 17. 主力分點 - 近30日 [來源：FinMind] ▼▼▼", df_b_30)
            render_html_table("▼▼▼ 18. 主力分點 - 近60日 [來源：FinMind] ▼▼▼", df_b_60)
            render_html_table("▼▼▼ 19. 八大官股進出 (今日) [來源：FinMind] ▼▼▼", df_19)
            
            st.markdown("#### ▼▼▼ 20. 董監大股東質設明細 [來源：富邦證券] ▼▼▼")
            if df_pledge_detail.empty:
                st.warning("本檔股票近 3 年內無董監事或大股東質設紀錄")
            else:
                if not df_pledge_summary.empty:
                    st.write("**【目前質設餘額與斷頭預警】**")
                    st.markdown(df_pledge_summary.to_html(index=False, border=1), unsafe_allow_html=True)
                st.write("**【近3年質設異動明細】**")
                st.markdown(df_pledge_detail.to_html(index=False, border=1), unsafe_allow_html=True)
                
            render_html_table("▼▼▼ 21. CBAS 可轉債數據 [來源：FinMind] ▼▼▼", df_cbas)
            render_html_table("▼▼▼ 22. 台指期貨三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_fut_inst)
            render_html_table("▼▼▼ 23. 台指選擇權三大法人未平倉 (大盤) [來源：FinMind] ▼▼▼", df_opt_inst)
            
            st.markdown("#### ▼▼▼ 24. 買賣家數差明細 (手動) [來源：使用者輸入] ▼▼▼")
            if not bs_diff: st.warning("此區塊目前查無數據")
            else: st.info(f"使用者輸入數值：{bs_diff}")

            # Prompt 區
            st.divider()
            st.subheader("📋 【給 Gemini 的量化分析資料包】")
            st.info("💡 提示：點擊下方黑色區塊右上角的按鈕即可一鍵複製！")
            
            p = f"請依下面最新的盤後資料幫我分析 {stock_id} 的量化籌碼，必須以我給的資料優先使用。\n\n"
            p += format_to_gas(df_share, "1. 最新集保分級明細 [來源：FinMind]")
            p += format_to_gas(df_twse, "2. 鉅額交易明細 [來源：證交所]")
            p += format_to_gas(df_margin, "3. 散戶資券餘額 [來源：FinMind]")
            p += format_to_gas(df_sbl, "4. 借券賣出與餘額 [來源：FinMind]")
            p += format_to_gas(df_daytrade, "5. 當沖明細 [來源：FinMind]")
            p += format_to_gas(df_inst, "6. 法人買賣超 [來源：FinMind]")
            p += format_to_gas(df_price, "7. 收盤價量 [來源：FinMind]")
            p += format_to_gas(df_rev, "8. 月營收 [來源：FinMind]")
            p += format_to_gas(df_div, "9. 歷年股利 [來源：FinMind]")
            p += format_to_gas(df_per, "10. 本益比、淨值比與殖利率 [來源：FinMind]")
            p += format_to_gas(df_disp, "11. 處置有價證券狀態 [來源：FinMind]")
            p += format_to_gas(df_b_today, f"12. 主力分點 - 今日 ({actual_dates[0]}) [來源：FinMind]")
            p += format_to_gas(df_b_prev1, f"13. 主力分點 - 前一日 ({actual_dates[1] if len(actual_dates)>1 else '無'}) [來源：FinMind]")
            p += format_to_gas(df_b_3, "14. 主力分點 - 近3日 [來源：FinMind]")
            p += format_to_gas(df_b_10, "15. 主力分點 - 近10日 [來源：FinMind]")
            p += format_to_gas(df_b_20, "16. 主力分點 - 近20日 [來源：FinMind]")
            p += format_to_gas(df_b_30, "17. 主力分點 - 近30日 [來源：FinMind]")
            p += format_to_gas(df_b_60, "18. 主力分點 - 近60日 [來源：FinMind]")
            p += format_to_gas(df_19, "19. 八大官股進出 (今日) [來源：FinMind]")
            p += format_pledge_to_gas(df_pledge_summary, df_pledge_detail)
            p += format_to_gas(df_cbas, "21. CBAS 可轉債數據 [來源：FinMind]")
            p += format_to_gas(df_fut_inst, "22. 台指期貨三大法人未平倉 (大盤) [來源：FinMind]")
            p += format_to_gas(df_opt_inst, "23. 台指選擇權三大法人未平倉 (大盤) [來源：FinMind]")
            p += f"▼▼▼ 24. 買賣家數差明細 (手動) [來源：使用者輸入] ▼▼▼, \n{bs_diff + ',' if bs_diff else '此區塊查無最新數據或無發行紀錄,'}\n"
            
            st.code(p, language="text")
