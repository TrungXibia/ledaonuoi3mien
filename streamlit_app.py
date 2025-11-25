import streamlit as st
import pandas as pd
import logic
import data_fetcher
import concurrent.futures
from datetime import datetime, timedelta
import importlib

importlib.reload(data_fetcher)
importlib.reload(logic)

STATION_ABBR = {
    "TP. Hồ Chí Minh": "HCM", "Đồng Tháp": "ĐT", "Cà Mau": "CM", "Bến Tre": "BT", "Vũng Tàu": "VT",
    "Bạc Liêu": "BL", "Đồng Nai": "ĐN", "Cần Thơ": "CT", "Sóc Trăng": "ST", "Tây Ninh": "TN",
    "An Giang": "AG", "Bình Thuận": "BTh", "Vĩnh Long": "VL", "Bình Dương": "BĐ", "Trà Vinh": "TV",
    "Long An": "LA", "Bình Phước": "BP", "Hậu Giang": "HG", "Tiền Giang": "TG", "Kiên Giang": "KG",
    "Đà Lạt": "ĐL", "Thừa Thiên Huế": "TTH", "Phú Yên": "PY", "Đắk Lắk": "ĐLk", "Quảng Nam": "QNa",
    "Đà Nẵng": "ĐN", "Khánh Hòa": "KH", "Bình Định": "BĐ", "Quảng Trị": "QT", "Quảng Bình": "QB",
    "Gia Lai": "GL", "Ninh Thuận": "NT", "Đắk Nông": "ĐNo", "Quảng Ngãi": "QNg", "Kon Tum": "KT"
}

st.set_page_config(page_title="SIÊU GÀ APP - PRO", page_icon="🐔", layout="wide")

st.markdown("""
<style>
    .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }
    [data-testid="column"] { padding: 0 0.3rem !important; }
    .table-wrapper { overflow-x: auto; margin: 10px 0; box-shadow: 0 1px 4px rgba(0,0,0,0.1); }
    .tracking-table { border-collapse: collapse; width: 100%; max-width: 800px; margin: 0 auto; font-size: 12px; }
    .tracking-table th { padding: 8px 4px; border: 1px solid #34495e; background-color: #2c3e50; color: white; text-align: center; position: sticky; top: 0; }
    .tracking-table td { padding: 4px; border: 1px solid #dee2e6; text-align: center; }
    .tracking-table td.moc-col { font-weight: bold; background-color: #f8f9fa; color: #d35400; }
    .cell-hit { background-color: #28a745 !important; color: white; font-weight: bold; }
    .cell-miss { background-color: #fff; color: #ccc; }
    @media (max-width: 768px) { .tracking-table { font-size: 10px; } }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=1800)
def get_master_data(num_days):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_dt = executor.submit(data_fetcher.fetch_dien_toan, num_days)
        f_tt = executor.submit(data_fetcher.fetch_than_tai, num_days)
        f_mb = executor.submit(data_fetcher.fetch_xsmb_group, num_days)
        
        dt = f_dt.result()
        tt = f_tt.result()
        mb_db, mb_g1, mb_g7 = f_mb.result()

    df_dt = pd.DataFrame(dt)
    df_tt = pd.DataFrame(tt)
    
    xsmb_rows = []
    limit = min(len(dt), len(mb_db), len(mb_g1), len(mb_g7))
    for i in range(limit):
        xsmb_rows.append({
            "date": dt[i]["date"],
            "xsmb_full": mb_db[i],
            "xsmb_2so": mb_db[i][-2:] if mb_db[i] else "",
            "g1_full": mb_g1[i],
            "g1_2so": mb_g1[i][-2:] if mb_g1[i] else "",
            "g7_list": mb_g7[i]
        })
    df_xsmb = pd.DataFrame(xsmb_rows)

    if not df_dt.empty and not df_xsmb.empty:
        df = pd.merge(df_dt, df_tt, on="date", how="left")
        df = pd.merge(df, df_xsmb, on="date", how="left")
        return df
    return pd.DataFrame()

with st.sidebar:
    st.title("🐔 SIÊU GÀ TOOL")
    days_fetch = st.number_input("Số ngày tải:", 30, 365, 60, step=10)
    days_show = st.slider("Hiển thị:", 10, 100, 20)
    if st.button("🔄 Tải lại dữ liệu", type="primary"):
        st.cache_data.clear()
        st.rerun()

try:
    with st.spinner("🚀 Đang tải dữ liệu đa luồng..."):
        df_full = get_master_data(days_fetch)
        if df_full.empty:
            st.error("Không có dữ liệu.")
            st.stop()
except Exception as e:
    st.error(f"Lỗi: {e}")
    st.stop()

df_show = df_full.head(days_show).copy()

st.title("🎯 DÀN LÔ GHÉP (MATRIX)")
st.caption("Ghép liên tiếp + Đảo (VD: 123 -> 12,21,23,32)")
st.divider()

c1, c2 = st.columns([1, 1])
src_mode = c1.selectbox("Nguồn:", ["Thần Tài", "Điện Toán"])
region = c2.selectbox("Miền:", ["Miền Bắc", "Miền Nam", "Miền Trung"])

selected_station = None
target_col = None

if region == "Miền Bắc":
    c3, c4, c5 = st.columns([1.5, 1.5, 1.5])
    comp_mode = c3.selectbox("So với:", ["XSMB (ĐB)", "Giải Nhất", "Giải 7"])
    check_range = c4.slider("Khung nuôi (ngày):", 1, 20, 5)
    backtest_mode = c5.selectbox("Backtest:", ["Hiện tại", "Lùi 1 ngày", "Lùi 2 ngày", "Lùi 3 ngày"])
    
    df_display = df_full
    df_check_source = df_full
    
    if comp_mode == "XSMB (ĐB)":
        target_col = "xsmb_2so"
    elif comp_mode == "Giải Nhất":
        target_col = "g1_2so"
    else:
        target_col = "g7_list"

else:
    c3, c4, c5, c6, c7 = st.columns([1, 1.2, 0.8, 1, 1])
    weekdays = ["Tất cả", "Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"]
    selected_day = c3.selectbox("Thứ:", weekdays)
    
    if selected_day == "Tất cả":
        selected_station = "Tất cả"
        c4.selectbox("Đài:", ["Tất cả"], disabled=True)
    else:
        stations = data_fetcher.get_stations_by_day(region, selected_day)
        station_options = ["Tất cả"] + stations
        selected_station = c4.selectbox("Đài:", station_options)
    
    prize_mode = c5.selectbox("Giải:", ["ĐB", "G1", "G8", "G7", "G6"])
    check_range = c6.slider("Khung:", 1, 20, 5)
    backtest_mode = c7.selectbox("Backtest:", ["Hiện tại", "Lùi 1", "Lùi 2", "Lùi 3"])

    prize_map = {
        "ĐB": "db_2so", "G1": "g1_2so", 
        "G8": "g8_2so", "G7": "g7_2so", "G6": "g6_2so"
    }
    target_col = prize_map[prize_mode]

    if selected_station == "Tất cả":
        all_stations = data_fetcher.get_all_stations_in_region(region)
        with st.spinner(f"🔄 Đang tải dữ liệu {region}..."):
            all_station_data = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_station = {executor.submit(data_fetcher.fetch_station_data, s, days_fetch): s for s in all_stations}
                for future in concurrent.futures.as_completed(future_to_station):
                    try:
                        data = future.result()
                        for item in data: item['station'] = future_to_station[future]
                        all_station_data.extend(data)
                    except: pass
            
            df_temp = pd.DataFrame(all_station_data)
            grouped_data = []
            if not df_temp.empty:
                for date, group in df_temp.groupby('date'):
                    day_vals = []
                    for _, row in group.iterrows():
                        val = row.get(target_col, [])
                        if isinstance(val, str) and val: day_vals.append(val)
                        elif isinstance(val, list): day_vals.extend(val)
                    
                    if day_vals:
                        grouped_data.append({'date': date, 'results': day_vals})
            
            df_check_source = pd.DataFrame(grouped_data)
            if not df_check_source.empty:
                df_check_source['date_obj'] = pd.to_datetime(df_check_source['date'], format='%d/%m/%Y')
                df_check_source = df_check_source.sort_values('date_obj', ascending=False)
                df_check_source = df_check_source.drop(columns=['date_obj'])

            if selected_day != "Tất cả":
                WEEKDAY_MAP = {"Thứ 2": 0, "Thứ 3": 1, "Thứ 4": 2, "Thứ 5": 3, "Thứ 6": 4, "Thứ 7": 5, "Chủ Nhật": 6}
                t_wd = WEEKDAY_MAP.get(selected_day)
                df_display = df_check_source[df_check_source['date'].apply(
                    lambda x: datetime.strptime(x, "%d/%m/%Y").weekday() == t_wd
                )].copy() if not df_check_source.empty else pd.DataFrame()
            else:
                df_display = df_check_source.copy()

    else:
        with st.spinner(f"🔄 Đang tải {selected_station}..."):
            station_data = data_fetcher.fetch_station_data(selected_station, total_days=days_fetch)
            df_temp = pd.DataFrame(station_data)
            df_temp['results'] = df_temp[target_col].apply(
                lambda x: [x] if isinstance(x, str) and x else (x if isinstance(x, list) else [])
            )
            df_display = df_temp[['date', 'results']]
            df_check_source = df_display

backtest_offset = int(backtest_mode.split()[1]) if "Lùi" in backtest_mode else 0
all_days_data = []

df_full_lookup = df_full.set_index('date') if not df_full.empty else pd.DataFrame()

if df_display is not None and not df_display.empty:
    start_idx = backtest_offset
    end_idx = min(backtest_offset + 30, len(df_display))
    
    for i in range(start_idx, end_idx):
        row = df_display.iloc[i]
        date_val = row['date']
        
        row_src = df_full_lookup.loc[date_val] if date_val in df_full_lookup.index else None
        if isinstance(row_src, pd.DataFrame): row_src = row_src.iloc[0]
        
        if row_src is None: continue
        
        src_str = ""
        if src_mode == "Thần Tài":
            src_str = str(row_src.get('tt_number', ''))
        else:
            val = row_src.get('dt_numbers', [])
            src_str = "".join(val) if isinstance(val, list) else (str(val) if pd.notna(val) else "")
            
        if not src_str or src_str == "nan": continue
        
        combos = logic.tao_dan_lien_tiep(src_str)
        if not combos: continue

        current_results = []
        if region == "Miền Bắc":
            val = row.get(target_col)
            if isinstance(val, list): current_results = val
            elif val: current_results = [val]
        else:
            current_results = row.get('results', [])

        all_days_data.append({
            'date': date_val,
            'source': src_str,
            'combos': combos,
            'index': i,
            'results': current_results
        })

if not all_days_data:
    st.warning("⚠️ Không có dữ liệu phù hợp.")
else:
    st.markdown("### 📋 Bảng Theo Dõi Lô Ghép")
    
    MAX_COLS = 10
    check_source_lookup = df_check_source.set_index('date') if df_check_source is not None else pd.DataFrame()
    
    table_html = "<div class='table-wrapper'><table class='tracking-table'><thead><tr>"
    table_html += "<th>Ngày</th><th>Nguồn</th><th>Dàn số</th>"
    for k in range(1, MAX_COLS + 1): table_html += f"<th>N{k}</th>"
    table_html += "</tr></thead><tbody>"
    
    for row_idx, day_data in enumerate(all_days_data):
        date, source, combos, i = day_data['date'], day_data['source'], day_data['combos'], day_data['index']
        
        table_html += "<tr>"
        table_html += f"<td>{date}</td>"
        table_html += f"<td class='moc-col'>{source}</td>"
        table_html += f"<td style='text-align:left; max-width:150px; word-wrap:break-word;'>{', '.join(combos)}</td>"
        
        num_cols_this_row = min(row_idx + 1, MAX_COLS)
        
        for k in range(1, MAX_COLS + 1):
            if k > num_cols_this_row:
                table_html += "<td style='background-color:#f8f9fa'></td>"
                continue
                
            check_results = []
            if selected_station == "Tất cả" and region != "Miền Bắc":
                try:
                    chk_date = (datetime.strptime(date, "%d/%m/%Y") + timedelta(days=k)).strftime("%d/%m/%Y")
                    if chk_date in check_source_lookup.index:
                        r = check_source_lookup.loc[chk_date]
                        if isinstance(r, pd.DataFrame): r = r.iloc[0]
                        check_results = r.get('results', [])
                except: pass
            else:
                chk_idx = i - k
                if chk_idx >= 0 and chk_idx < len(df_display):
                    r = df_display.iloc[chk_idx]
                    if region == "Miền Bắc":
                        val = r.get(target_col)
                        check_results = val if isinstance(val, list) else ([val] if val else [])
                    else:
                        check_results = r.get('results', [])
            
            hit_nums = set(combos) & set(check_results)
            
            if hit_nums:
                table_html += f"<td class='cell-hit'>{list(hit_nums)[0]}</td>"
            elif check_results:
                table_html += "<td class='cell-miss'>-</td>"
            else:
                table_html += "<td></td>"
                
        table_html += "</tr>"
    table_html += "</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)

    st.markdown("#### 💡 Gợi ý dàn ngày mai")
    if all_days_data:
        latest = all_days_data[0]
        st.info(f"Ngày **{latest['date']}** - Nguồn **{latest['source']}**")
        st.success(f"Dàn nuôi: **{', '.join(latest['combos'])}**")
        st.caption(f"Kiểm tra với: {region} - {selected_station if region != 'Miền Bắc' else ''} - {comp_mode if region == 'Miền Bắc' else prize_mode}")
