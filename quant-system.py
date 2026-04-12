import streamlit as st
import requests
import pandas as pd
import numpy as np
import datetime
from io import StringIO
import re
import concurrent.futures
import urllib.request
import ssl
import urllib3

# 📌 關閉所有憑證警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設定網頁標題與佈局
st.set_page_config(page_title="台股全息量化系統", layout="wide")

# 內建最新 Sponsor Token
FINMIND_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJkYXRlIjoiMjAyNi0wNC0xMCAyMDoyMDo0NiIsInVzZXJfaWQiOiJUb25lMSIsImVtYWlsIjoidG9uZWhzaWVAZ21haWwuY29tIiwiaXAiOiI2MS42Mi43LjE5OCJ9.7s3-IrkfdiUyTvGiZQGESBUBAPHQTnd4pwYcn8_J-CY"

# 📌 注入全局 CSS
st.markdown("""
<style>
table.dataframe { width: 100% !important; border-collapse: collapse !important; }
table.dataframe th { text-align: center !important; background-color: #f0f2f6; }
table.radar-table td:last-child { text-align: left !important; color: #1f77b4; font-weight: bold; }
.stExpander { border: 1px solid #d1d1d1; border-radius: 5px; }
</style>
""", unsafe_allow_html=True)

# 專業主標題
st.title("🤖 交易員實戰手冊：全息量化擷取系統")

# UI 輸入區 
col1, col2 = st.columns([1, 1])
with col1:
    user_stock_id = st.text_input("個股代號", value="1785")
with col2:
    dead_chip_input = st.text_input("死籌碼 %", placeholder="系統自動抓取，亦可手動調整比例。", help="預設抓取全體董監持股。")

st.write("")
run_btn = st.button("🚀 啟動引擎：擷取全息資料並產生 Prompt", use_container_width=True)

st.divider()

# ==========================================
# 工具函式與多重死籌碼引擎
# ==========================================

def safe_float(x):
    try:
        return float(str(x).replace(',', '').replace('%', '').strip())
    except:
        return 0.0

def get_stock_name(target_id):
    try:
        res = requests.get(f"https://tw.stock.yahoo.com/quote/{target_id}.TW", headers={"User-Agent": "Mozilla/5.0"}, timeout=5)
        match = re.search(r'<title>(.*?)\s*\(', res.text)
        return match.group(1).strip() if match else ""
    except: return ""

def safe_get_fubon(url):
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        if hasattr(ssl, 'OP_LEGACY_SERVER_CONNECT'):
            ctx.options |= ssl.OP_LEGACY_SERVER_CONNECT
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            return response.read().decode('big5', errors='ignore')
    except:
        try:
            res = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10, verify=False)
            res.encoding = 'big5'
            return res.text
        except: return ""

def fetch_fm(dataset, start_date, target_id=None):
    url = "https://api.finmindtrade.com/api/v4/data"
    params = {"dataset": dataset, "start_date": start_date}
    if target_id: params["data_id"] = target_id
    headers = {"Authorization": f"Bearer {FINMIND_TOKEN}"}
    try:
        res = requests.get(url, params=params, headers=headers, timeout=15).json()
        return pd.DataFrame(res.get("data", []))
    except: return pd.DataFrame()

def format_to_gas(df, title):
    if df is None or df.empty: return f"▼▼▼ {title} ▼▼▼, \n查無數據, \n"
    header = f"▼▼▼ {title} ▼▼▼, \n"
    csv_str = df.to_csv(index=False)
    lines = [line.replace('"', '') + ", " for line in csv_str.strip().split('\n')]
    return header + "\n".join(lines) + "\n"

def scrape_director_holding(target_id):
    debug_log = []
    dynamic_dict = {}
    try:
        url = f"https://goodinfo.tw/tw/StockDirectorSharehold.asp?STOCK_ID={target_id}"
        h = {"User-Agent": "Mozilla/5.0", "Referer": "https://goodinfo.tw", "Cookie": "CLIENT_KEY=20260411;"}
        res = requests.get(url, headers=h, timeout=8)
        if res.status_code == 200:
            res.encoding = 'utf-8'
            dfs = pd.read_html(StringIO(res.text))
            for df in dfs:
                target_col = next((c for c in df.columns if '全體董監持股' in str(c) and '持股(%)' in str(c)), None)
                month_col = next((c for c in df.columns if '月別' in str(c)), None)
                if target_col and month_col:
                    latest_val = 0.0
                    for _, row in df.iterrows():
                        m = str(row[month_col]).replace('/', '-').strip()
                        v = safe_float(row[target_col])
                        if re.match(r'^\d{4}-\d{2}$', m) and v > 0:
                            dynamic_dict[m] = v
                            if latest_val == 0.0: latest_val = v
                    return dynamic_dict, latest_val, "Goodinfo"
    except Exception as e: debug_log.append(str(e))
    return {}, 0.0, "失敗"

def get_dead_chip_val(date_str, dead_input, d_dict, s_val, engine):
    if dead_input and str(dead_input).strip() != "":
        return safe_float(dead_input), "手動"
    m_key = str(date_str)[:7].replace('/', '-')
    if d_dict and m_key in d_dict: return d_dict[m_key], "Goodinfo當月"
    if d_dict: return list(d_dict.values())[0], "Goodinfo最新"
    return s_val, engine

# ==========================================
# 核心處理引擎
# ==========================================

def process_tdcc(df):
    if df.empty: return [pd.DataFrame()]*5
    df = df[~df['HoldingSharesLevel'].astype(str).str.contains('差異數')]
    
    def clean_lv(x):
        nums = re.findall(r'\d+', str(x).replace(',', ''))
        if not nums: return "合計"
        up = int(nums[-1])
        m_ranges = [(999, "1-999股"), (5000, "1-5張"), (10000, "5-10張"), (15000, "10-15張"), (20000, "15-20張"), (30000, "20-30張"), (40000, "30-40張"), (50000, "40-50張"), (100000, "50-100張"), (200000, "100-200張"), (400000, "200-400張"), (600000, "400-600張"), (800000, "600-800張"), (1000000, "800-1000張")]
        for limit, label in m_ranges:
            if up <= limit: return label
        return "1000張以上"

    df['LevelClean'] = df['HoldingSharesLevel'].apply(clean_lv)
    df['people'] = pd.to_numeric(df['people'], errors='coerce').fillna(0).astype(int)
    df['percent'] = pd.to_numeric(df['percent'], errors='coerce').fillna(0)
    df['unit'] = (pd.to_numeric(df.get('unit', 0), errors='coerce').fillna(0) / 1000).round().astype(int)
    
    dates = sorted(df['date'].unique(), reverse=True)[:10]
    df = df[df['date'].isin(dates)]
    lvls = ['1-999股', '1-5張', '5-10張', '10-15張', '15-20張', '20-30張', '30-40張', '40-50張', '50-100張', '100-200張', '200-400張', '400-600張', '600-800張', '800-1000張', '1000張以上']
    
    p_unit = df[~df['LevelClean'].str.contains('合計')].pivot_table(index='date', columns='LevelClean', values='unit', aggfunc='first').reindex(columns=lvls, fill_value=0)
    p_pct = df[~df['LevelClean'].str.contains('合計')].pivot_table(index='date', columns='LevelClean', values='percent', aggfunc='first').reindex(columns=lvls, fill_value=0)
    p_people = df[~df['LevelClean'].str.contains('合計')].pivot_table(index='date', columns='LevelClean', values='people', aggfunc='first').reindex(columns=lvls, fill_value=0)

    df_total = pd.DataFrame({'日期': p_unit.index})
    df_total['總張數'] = p_unit.sum(axis=1).values
    df_total['總人數(人)'] = p_people.sum(axis=1).values
    df_total['總均張'] = (df_total['總張數']/df_total['總人數(人)'].replace(0, np.nan)).fillna(0).round(2)
    
    df_wide = df_total.copy()
    for l in lvls:
        df_wide[f"{l}_比例(%)"] = p_pct[l].values
    
    return [df_wide.sort_values('日期', ascending=False), p_unit.reset_index().sort_values('date', ascending=False), p_people.reset_index().sort_values('date', ascending=False), p_pct.reset_index().sort_values('date', ascending=False), df_total.sort_values('日期', ascending=False)]

def get_expert_radar(df_wide, df_price, dead_input, d_dict, s_val, engine):
    if df_wide.empty or len(df_wide) < 2: return pd.DataFrame()
    df = df_wide.sort_values('日期', ascending=True).copy()
    df['dt'] = pd.to_datetime(df['日期'])
    df_p = df_price.copy(); df_p['dt'] = pd.to_datetime(df_p['日期'])
    df = pd.merge_asof(df, df_p[['dt', '收盤價(元)']], on='dt', direction='backward')
    
    df['1000張變動(%)'] = df['1000張以上_比例(%)'].diff().round(2)
    df['作戰區變動(%)'] = (df['200-400張_比例(%)']+df['400-600張_比例(%)']+df['600-800張_比例(%)']).diff().round(2)
    df['總人數變動率(%)'] = (df['總人數(人)'].pct_change() * 100).round(2)
    
    def diagnose(row):
        dead_v, _ = get_dead_chip_val(row['日期'], dead_input, d_dict, s_val, engine)
        lev = 100/(100-dead_v) if 0 < dead_v < 100 else 1
        r1000 = row['1000張變動(%)'] * lev
        advice = []
        if row.get('收盤價(元)', 0) < 35 and row['1000張變動(%)'] >= 0.8: advice.append("💎 [鐵桿鎖碼]")
        if row['總人數變動率(%)'] > 2.5 and r1000 < -0.5: return "💀 [逃命警報]"
        if r1000 > 2.5 and row['總人數變動率(%)'] < 0: advice.append("🚀 [暴力軋空]")
        if row['作戰區變動(%)'] > 0.5: advice.append("🔴 [分身集結]")
        return " | ".join(advice) if advice else "🔵 趨勢盤整"

    df['V24_實戰診斷'] = df.apply(diagnose, axis=1)
    return df[['日期', '收盤價(元)', '總人數變動率(%)', '1000張變動(%)', '作戰區變動(%)', 'V24_實戰診斷']].sort_values('日期', ascending=False).head(10)

# ==========================================
# 基礎資料處理
# ==========================================

def process_price(df):
    if df.empty: return pd.DataFrame()
    df['Trading_Volume'] = (pd.to_numeric(df['Trading_Volume'], errors='coerce') / 1000).fillna(0).round().astype(int)
    df = df.rename(columns={"date":"日期","Trading_Volume":"成交量(張)","close":"收盤價(元)","spread":"漲跌(元)"})
    df["斷頭價(0.78)"] = (df["收盤價(元)"] * 0.78).round(2)
    return df[['日期','成交量(張)','收盤價(元)','漲跌(元)','斷頭價(0.78)']].sort_values('日期', ascending=False)

def process_inst(df):
    if df.empty: return pd.DataFrame()
    p = df.pivot_table(index='date', columns='name', values=['buy', 'sell'], fill_value=0).reset_index()
    p.columns = ['_'.join(c).strip('_') for c in p.columns.values]
    out = pd.DataFrame({'日期': p['date']})
    f_net = (p.get('buy_Foreign_Investor',0) + p.get('buy_Foreign_Dealer_Self',0)) - (p.get('sell_Foreign_Investor',0) + p.get('sell_Foreign_Dealer_Self',0))
    it_net = p.get('buy_Investment_Trust',0) - p.get('sell_Investment_Trust',0)
    out['外資買賣超(張)'] = (f_net / 1000).round().astype(int)
    out['投信買賣超(張)'] = (it_net / 1000).round().astype(int)
    return out.sort_values('日期', ascending=False).head(15)

# ==========================================
# 執行與呈現
# ==========================================

if run_btn:
    with st.spinner(f"正在擷取 {user_stock_id} 數據..."):
        name = get_stock_name(user_stock_id)
        start_date = (datetime.date.today() - datetime.timedelta(days=1000)).strftime("%Y-%m-%d")
        df_p_raw = fetch_fm("TaiwanStockPrice", start_date, user_stock_id)
        if df_p_raw.empty: st.error("查無股價資料"); st.stop()
        
        dates = sorted(df_p_raw['date'].unique().tolist(), reverse=True)
        d60 = dates[59] if len(dates) >= 60 else dates[-1]
        
        df_price = process_price(df_p_raw)
        ddict, sval, engine = scrape_director_holding(user_stock_id)
        
        df_share_raw = fetch_fm("TaiwanStockHoldingSharesPer", d60, user_stock_id)
        df_s_wide, df_s_unit, df_s_people, df_s_pct, df_s_total = process_tdcc(df_share_raw)
        df_radar = get_expert_radar(df_s_wide, df_price, dead_chip_input, ddict, sval, engine)
        df_inst = process_inst(fetch_fm("TaiwanStockInstitutionalInvestorsBuySell", d60, user_stock_id))

        # 資料呈現
        st.markdown("#### ▼▼▼ 1. 專家診斷雷達 ▼▼▼")
        st.markdown(df_radar.to_html(index=False, classes="dataframe radar-table"), unsafe_allow_html=True)
        
        st.markdown("#### ▼▼▼ 2. 收盤價量與斷頭預警 ▼▼▼")
        st.markdown(df_price.head(15).to_html(index=False, classes="dataframe"), unsafe_allow_html=True)

        st.markdown("#### ▼▼▼ 3. 法人買賣超 (張) ▼▼▼")
        st.markdown(df_inst.to_html(index=False, classes="dataframe"), unsafe_allow_html=True)

        st.markdown("#### ▼▼▼ 4. 集保分級比例表 (%) ▼▼▼")
        st.markdown(df_s_pct.sort_values('date', ascending=False).to_html(index=False, classes="dataframe"), unsafe_allow_html=True)

        st.divider()
        with st.expander("📋 【點擊展開：給 Gemini 的量化分析資料包】", expanded=False):
            p = f"請分析 {user_stock_id} {name} 的籌碼：\n\n"
            p += format_to_gas(df_radar, "1. 專家診斷雷達")
            p += format_to_gas(df_price.head(15), "2. 收盤價量")
            p += format_to_gas(df_inst, "3. 法人買賣超")
            p += format_to_gas(df_s_wide.head(10), "4. 集保大戶變動")
            st.code(p, language="text")
