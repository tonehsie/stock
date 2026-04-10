def process_tdcc_dynamic(df_share, df_price, dead_chip_str, base_money_str, influence_pct_str):
    if df_share.empty or df_price.empty: return pd.DataFrame()
    
    try: dead_chip_pct = float(dead_chip_str) / 100.0 if dead_chip_str else 0.0
    except: dead_chip_pct = 0.0
    try: base_money_wan = float(base_money_str) if base_money_str else 5000.0
    except: base_money_wan = 5000.0
    try: influence_rate = float(influence_pct_str) / 100.0 if influence_pct_str else 0.005
    except: influence_rate = 0.005
    
    df_share['dt'] = pd.to_datetime(df_share['日期']); df_price['dt'] = pd.to_datetime(df_price['日期'])
    df_m = pd.merge_asof(df_share.sort_values('dt'), df_price.sort_values('dt')[['dt', '收盤價(元)']], on='dt', direction='backward').sort_values('dt', ascending=False)
    
    out = []
    for _, row in df_m.iterrows():
        p = row['收盤價(元)']
        if pd.isna(p) or p == 0: continue
        total_units = row.get('總張數', 0)
        cap_b = total_units / 10000
        
        # 1. 雙軸取大值門檻 (財力 vs 影響力)
        money_threshold = (base_money_wan * 10000) / (p * 1000)
        influence_threshold = total_units * influence_rate
        raw_t = max(money_threshold, influence_threshold)
        
        # 2. 無條件進位至大戶級距 (確保具備實質影響力)
        valid_thresholds = [100, 200, 400, 600, 800, 1000]
        ceiling_t = 1000 # 預設最大值
        for t in valid_thresholds:
            if t >= raw_t:
                ceiling_t = t
                break
        
        # 3. 取得該級距以上之所有籌碼佔比
        large_cols = []
        if ceiling_t <= 100: large_cols = ['100-200張_比例(%)', '200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 200: large_cols = ['200-400張_比例(%)', '400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 400: large_cols = ['400-600張_比例(%)', '600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 600: large_cols = ['600-800張_比例(%)', '800-1000張_比例(%)', '1000張以上_比例(%)']
        elif ceiling_t <= 800: large_cols = ['800-1000張_比例(%)', '1000張以上_比例(%)']
        else: large_cols = ['1000張以上_比例(%)']

        large_pct = sum([pd.to_numeric(row.get(c, 0), errors='coerce') for c in large_cols])
        large_pct = 0 if pd.isna(large_pct) else large_pct
        
        # 4. 活大戶影響力 C-Value 計算與狀態判定
        if dead_chip_pct > 0 and dead_chip_pct < 1:
            active_pool = 1.0 - dead_chip_pct
            c_val = ((large_pct / 100.0) - dead_chip_pct) / active_pool
            c_val = max(0, c_val) # 避免 C-Value 出現負數
            
            if c_val > 0.5: status = "🔴 絕對控盤"
            elif c_val >= 0.3: status = "🟡 高度鎖碼"
            elif c_val >= 0.15: status = "🔵 初步集結"
            else: status = "⚪ 籌碼渙散"
        else:
            c_val = large_pct / 100.0
            status = "未輸入死籌碼"

        out.append({
            "日期": row['日期'], 
            "收盤價": p, 
            "股本(億)": round(cap_b, 2),
            "主導門檻": "影響力" if influence_threshold > money_threshold else "財力",
            "精算門檻(張)": ceiling_t, 
            "級距總佔比(%)": round(large_pct, 2),
            "死籌碼(%)": int(dead_chip_pct * 100) if dead_chip_str else "-",
            "活大戶影響力C(%)": round(c_val * 100, 1) if dead_chip_str else "-",
            "實戰判定": status
        })
    return pd.DataFrame(out)
