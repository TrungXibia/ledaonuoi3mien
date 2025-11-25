import streamlit as st
import pandas as pd
import logic
import data_fetcher
import concurrent.futures
from datetime import datetime, timedelta
import importlib
importlib.reload(data_fetcher)

# --- CẤU HÌNH ---
st.set_page_config(
    page_title="SIÊU GÀ APP - PRO",
    page_icon="🐔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS FIX (Tối ưu hiển thị cột Dàn số) ---
st.markdown("""
<style>
    .block-container { padding-top: 1rem !important; padding-bottom: 1rem !important; }
    [data-testid="column"] { padding: 0 0.3rem !important; }
    
    /* Wrapper cho phép cuộn ngang */
    .table-wrapper { 
        overflow-x: auto; 
        margin: 10px 0; 
        border-radius: 6px; 
        box-shadow: 0 1px 4px rgba(0,0,0,0.1); 
        border: 1px solid #eee;
    }
    
    .tracking-table { 
        border-collapse: collapse; 
        width: 100%; 
        min-width: 1000px; /* Tăng chiều rộng tối thiểu vì thêm cột Dàn */
        margin: 0 auto; 
        font-size: 10px; 
    }
    
    .tracking-table th { 
        padding: 4px 2px; 
        border: 1px solid #34495e; 
        background-color: #2c3e50; 
        color: white; 
        text-align: center; 
        position: sticky; 
        top: 0; 
        z-index: 10; 
        font-weight: 600; 
        min-width: 25px; 
    }
    
    .tracking-table td { 
        padding: 2px 0px; 
        border: 1px solid #dee2e6; 
        text-align: center; 
        min-width: 25px;
    }
    
    .tracking-table td.moc-col { 
        font-weight: bold; 
        background-color: #f8f9fa; 
        color: #d63031; /* Màu đỏ cho số mốc */
        min-width: 40px;
        font-size: 11px;
    }
    
    /* CSS cho cột Dàn số mới */
    .tracking-table td.dan-col {
        font-size: 9px;
        text-align: left;
        padding: 2px 4px;
        min-width: 120px;
        max-width: 200px;
        white-space: normal; /* Cho phép xuống dòng */
        color: #2d3436;
        background-color: #fff;
        line-height: 1.2;
    }
    
    .tracking-table td.date-col {
        min-width: 60px;
        color: #666;
    }

    .cell-hit { background-color: #28a745 !important; color: white; font-weight: bold; font-size: 14px; }
    .cell-miss { background-color: #dc3545 !important; color: white; font-size: 12px; }
</style>
""", unsafe_allow_html=True)

# --- QUẢN LÝ DỮ LIỆU ---
@st.cache_data(ttl=1800)
def get_master_data(num_days):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_dt = executor.submit(data_fetcher.fetch_dien_toan, num_days)
        f_tt = executor.submit(data_fetcher.fetch_than_tai, num_days)
        f_mb = executor.submit(data_fetcher.fetch_xsmb_full, num_days)
        
        dt = f_dt.result()
        tt = f_tt.result()
        mb = f_mb.result()

    df_dt = pd.DataFrame(dt)
    df_tt = pd.DataFrame(tt)
    df_mb = pd.DataFrame(mb)
    
    if not df_mb.empty:
        df_mb = df_mb.rename(columns={
            "g6_list": "mb_g6_list", 
            "g7_list": "mb_g7_list", 
            "db_2so": "mb_db_2so"
        })
        df_mb = df_mb[["date", "mb_db_2so", "mb_g6_list", "mb_g7_list"]]

    if not df_dt.empty:
        df = pd.merge(df_dt, df_tt, on="date", how="left")
        if not df_mb.empty:
            df = pd.merge(df, df_mb, on="date", how="left")
        return df
    return pd.DataFrame()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🐔 SIÊU GÀ TOOL")
    st.caption("Version: 20 Days + Detail Column")
    days_fetch = st.number_input("Số ngày tải:", 30, 365, 60, step=10)
    days_show = st.slider("Hiển thị dòng:", 10, 100, 20)
    if st.button("🔄 Tải lại dữ liệu", type="primary"):
        st.cache_data.clear()
        st.rerun()

# --- LOAD DATA ---
try:
    with st.spinner("🚀 Đang tải dữ liệu đa luồng..."):
        df_full = get_master_data(days_fetch)
        if df_full.empty:
            st.error("Không có dữ liệu. Kiểm tra kết nối mạng.")
            st.stop()
except Exception as e:
    st.error(f"Lỗi: {e}")
    st.stop()

# === 🎯 DÀN NUÔI (MATRIX) ===
st.title("🎯 DÀN NUÔI (MATRIX 20 NGÀY)")
st.divider()

# Row 1: Nguồn và Miền
c1, c2 = st.columns([1, 1])
src_mode = c1.selectbox("Nguồn (Dàn gốc):", ["Thần Tài", "Điện Toán"])
region = c2.selectbox("Miền:", ["Miền Bắc", "Miền Nam", "Miền Trung"])

# Row 2: Logic Selection
selected_station = None
check_mode_desc = ""

if region == "Miền Bắc":
    c3, c4, c5 = st.columns([1.5, 1.5, 1.5])
    c3.selectbox("So với:", ["G6 + G7"], disabled=True)
    check_mode_desc = "G6 + G7"
    check_range = c4.slider("Khung nuôi (hiển thị N):", 1, 20, 20)
    backtest_mode = c5.selectbox("Backtest:", ["Hiện tại", "Lùi 1 ngày", "Lùi 2 ngày", "Lùi 3 ngày", "Lùi 4 ngày", "Lùi 5 ngày"])
    
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
    
    c5.selectbox("Giải:", ["G6+G7+G8"], disabled=True)
    check_mode_desc = "G6 + G7 + G8"
    check_range = c6.slider("Khung (N):", 1, 20, 20)
    backtest_mode = c7.selectbox("Backtest:", ["Hiện tại", "Lùi 1", "Lùi 2", "Lùi 3", "Lùi 4", "Lùi 5"])

backtest_offset = int(backtest_mode.split()[1]) if backtest_mode != "Hiện tại" else 0

# === FETCH DATA ===
df_display = None
df_check_source = None

if region == "Miền Bắc":
    df_display = df_full
    df_check_source = df_full
else:
    if selected_station == "Tất cả":
        all_stations = data_fetcher.get_all_stations_in_region(region)
        with st.spinner(f"🔄 Tải dữ liệu {region}..."):
            all_station_data = []
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_station = {executor.submit(data_fetcher.fetch_station_data, s, days_fetch): s for s in all_stations}
                for future in concurrent.futures.as_completed(future_to_station):
                    try:
                        data = future.result()
                        for item in data: item['station'] = future_to_station[future]
                        all_station_data.extend(data)
                    except: pass
            
            if not all_station_data: st.stop()
            
            df_temp = pd.DataFrame(all_station_data)
            grouped_data = []
            for date, group in df_temp.groupby('date'):
                day_values = []
                for _, row in group.iterrows():
                    day_values.extend(row.get('g6_list', []))
                    day_values.extend(row.get('g7_list', []))
                    day_values.extend(row.get('g8_list', []))
                
                if day_values:
                    grouped_data.append({'date': date, 'result_pool': list(set(day_values))})
            
            df_check_source = pd.DataFrame(grouped_data)
            df_check_source['date_obj'] = pd.to_datetime(df_check_source['date'], format='%d/%m/%Y')
            df_check_source = df_check_source.sort_values('date_obj', ascending=False).drop(columns=['date_obj'])
            
            if selected_day != "Tất cả":
                WEEKDAY_MAP = {"Thứ 2": 0, "Thứ 3": 1, "Thứ 4": 2, "Thứ 5": 3, "Thứ 6": 4, "Thứ 7": 5, "Chủ Nhật": 6}
                target = WEEKDAY_MAP.get(selected_day)
                df_display = df_check_source[df_check_source['date'].apply(lambda d: datetime.strptime(d, "%d/%m/%Y").weekday() == target)].copy()
            else:
                df_display = df_check_source.copy()
    else:
        with st.spinner(f"🔄 Tải {selected_station}..."):
            s_data = data_fetcher.fetch_station_data(selected_station, total_days=days_fetch)
            if not s_data: st.stop()
            p_data = []
            for item in s_data:
                pool = item.get('g6_list', []) + item.get('g7_list', []) + item.get('g8_list', [])
                p_data.append({'date': item['date'], 'result_pool': pool})
            df_display = pd.DataFrame(p_data)
            df_check_source = df_display

# === PREPARE ANALYSIS ===
all_days_data = []
start_idx = backtest_offset
end_idx = min(backtest_offset + days_show, len(df_display))
df_full_lookup = df_full.set_index('date') if not df_full.empty else pd.DataFrame()

for i in range(start_idx, end_idx):
    row = df_display.iloc[i]
    date_val = row['date']
    
    row_src = None
    if region == "Miền Bắc":
        row_src = row
    elif date_val in df_full_lookup.index:
        row_src = df_full_lookup.loc[date_val]
        if isinstance(row_src, pd.DataFrame): row_src = row_src.iloc[0]
    
    if row_src is None: continue

    src_str = ""
    if src_mode == "Thần Tài": 
        src_str = str(row_src.get('tt_number', ''))
    elif src_mode == "Điện Toán": 
        val = row_src.get('dt_numbers', [])
        if isinstance(val, list): src_str = "".join(val)
        else: src_str = str(val) if pd.notna(val) else ""
    
    if not src_str or src_str == "nan": continue
    
    combos = logic.tao_dan_nhi_hop_vong(src_str)
    
    if region == "Miền Bắc":
        pool = row.get('mb_g6_list', []) + row.get('mb_g7_list', [])
    else:
        pool = row.get('result_pool', [])
        
    all_days_data.append({
        'date': date_val, 'source': src_str, 'combos': combos, 
        'index': i, 'result_pool': pool
    })

# === RENDER MATRIX ===
if all_days_data:
    st.markdown(f"### 📋 Bảng Theo Dõi ({check_mode_desc})")
    MAX_COLS = 20
    check_source_lookup = df_check_source.set_index('date') if df_check_source is not None else pd.DataFrame()
    
    table_html = "<div class='table-wrapper'><table class='tracking-table'><thead><tr>"
    # Thêm cột "Dàn số" vào header
    table_html += "<th>Ngày</th><th>Mốc</th><th>Dàn số</th>"
    
    for k in range(1, MAX_COLS + 1): 
        table_html += f"<th>N{k}</th>"
    table_html += "</tr></thead><tbody>"
    
    for row_idx, day_data in enumerate(all_days_data):
        date, source, combos, i = day_data['date'], day_data['source'], day_data['combos'], day_data['index']
        
        # Chuyển list combos thành chuỗi string
        combos_str = ", ".join(combos)
        
        # Render hàng mới có thêm cột dan-col
        table_html += f"<tr><td class='date-col'>{date}</td><td class='moc-col'>{source}</td><td class='dan-col'>{combos_str}</td>"
        
        days_passed = row_idx + 1
        for k in range(1, MAX_COLS + 1):
            if k > days_passed:
                table_html += "<td style='background-color:#f8f9fa'></td>"
                continue
                
            check_pool = []
            if selected_station == "Tất cả" and region != "Miền Bắc":
                try:
                    c_date = (datetime.strptime(date, "%d/%m/%Y") + timedelta(days=k)).strftime("%d/%m/%Y")
                    if c_date in check_source_lookup.index:
                        r = check_source_lookup.loc[c_date]
                        if isinstance(r, pd.DataFrame): r = r.iloc[0]
                        check_pool = r.get('result_pool', [])
                except: pass
            else:
                idx = i - k
                if idx >= 0:
                    r = df_display.iloc[idx]
                    if region == "Miền Bắc":
                        check_pool = r.get('mb_g6_list', []) + r.get('mb_g7_list', [])
                    else:
                        check_pool = r.get('result_pool', [])

            is_hit = False
            for num in combos:
                if num in check_pool:
                    is_hit = True
                    break
            
            if is_hit: table_html += "<td class='cell-hit'>✓</td>"
            elif check_pool: table_html += "<td class='cell-miss'>−</td>"
            else: table_html += "<td>−</td>"
            
        table_html += "</tr>"
    table_html += "</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)
    
    # === STATISTICS ===
    st.markdown("---")
    st.caption("Ghi chú: Cột 'Dàn số' hiển thị chi tiết các số nhị hợp vòng được nuôi.")
    
    pending = []
    for row_idx, day_data in enumerate(all_days_data):
        combos = day_data['combos']
        has_hit = False
        num_checks = min(row_idx + 1, MAX_COLS)
        
        for k in range(1, num_checks + 1):
            check_pool = []
            if selected_station == "Tất cả" and region != "Miền Bắc":
                try:
                    c_date = (datetime.strptime(day_data['date'], "%d/%m/%Y") + timedelta(days=k)).strftime("%d/%m/%Y")
                    if c_date in check_source_lookup.index:
                        r = check_source_lookup.loc[c_date]
                        if isinstance(r, pd.DataFrame): r = r.iloc[0]
                        check_pool = r.get('result_pool', [])
                except: pass
            else:
                idx = day_data['index'] - k
                if idx >= 0:
                    r = df_display.iloc[idx]
                    if region == "Miền Bắc":
                        check_pool = r.get('mb_g6_list', []) + r.get('mb_g7_list', [])
                    else:
                        check_pool = r.get('result_pool', [])
            
            if any(c in check_pool for c in combos):
                has_hit = True
                break
        
        if not has_hit:
            pending.append({
                "Ngày": day_data['date'],
                "Dàn số": ", ".join(combos),
                "Số lượng": len(combos),
                "Đã nuôi": f"{num_checks} ngày"
            })

    if pending:
        st.subheader(f"🔥 Các Dàn Chưa Ra (Trong {MAX_COLS} ngày)")
        st.dataframe(pd.DataFrame(pending), use_container_width=True)
    else:
        st.success(f"Tuyệt vời! Tất cả các dàn đã nổ trong vòng {MAX_COLS} ngày.")
