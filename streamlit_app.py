import streamlit as st
import pandas as pd
import logic
import data_fetcher
import concurrent.futures
from datetime import datetime, timedelta
import importlib
importlib.reload(data_fetcher)

# Mapping viết tắt tên đài
STATION_ABBR = {
    "TP. Hồ Chí Minh": "HCM",
    "Đồng Tháp": "ĐT",
    "Cà Mau": "CM",
    "Bến Tre": "BT",
    "Vũng Tàu": "VT",
    "Bạc Liêu": "BL",
    "Đồng Nai": "ĐN",
    "Cần Thơ": "CT",
    "Sóc Trăng": "ST",
    "Tây Ninh": "TN",
    "An Giang": "AG",
    "Bình Thuận": "BTh",
    "Vĩnh Long": "VL",
    "Bình Dương": "BĐ",
    "Trà Vinh": "TV",
    "Long An": "LA",
    "Bình Phước": "BP",
    "Hậu Giang": "HG",
    "Tiền Giang": "TG",
    "Kiên Giang": "KG",
    "Đà Lạt": "ĐL",
    # Miền Trung
    "Thừa Thiên Huế": "TTH",
    "Phú Yên": "PY",
    "Đắk Lắk": "ĐLk",
    "Quảng Nam": "QNa",
    "Đà Nẵng": "ĐN",
    "Khánh Hòa": "KH",
    "Bình Định": "BĐ",
    "Quảng Trị": "QT",
    "Quảng Bình": "QB",
    "Gia Lai": "GL",
    "Ninh Thuận": "NT",
    "Đắk Nông": "ĐNo",
    "Quảng Ngãi": "QNg",
    "Kon Tum": "KT"
}

# --- CẤU HÌNH ---
st.set_page_config(
    page_title="SIÊU GÀ APP - PRO",
    page_icon="🐔",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- CSS FIX LỖI FONT & GIAO DIỆN + RESPONSIVE ---
st.markdown("""
<style>
    /* Tối ưu spacing */
    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 1rem !important;
    }
    
    /* Compact columns */
    [data-testid="column"] {
        padding: 0 0.3rem !important;
    }
    
    /* Fix lỗi font menu */
    .stTabs [data-baseweb="tab-list"] { gap: 4px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        background-color: #e0e0e0;
        border-radius: 5px 5px 0 0;
        padding: 10px;
        color: #000000 !important;
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        background-color: #ff4b4b !important;
        color: #ffffff !important;
    }
    
    /* VERTICAL TRACKING TABLE */
    .table-wrapper {
        overflow-x: auto;
        -webkit-overflow-scrolling: touch;
        margin: 10px 0;
        border-radius: 6px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.1);
    }
    
    .tracking-table {
        border-collapse: collapse;
        width: 100%;
        max-width: 650px;
        margin: 0 auto;
        font-size: 11px;
    }
    
    .tracking-table th {
        padding: 6px 4px;
        border: 1px solid #34495e;
        background-color: #2c3e50;
        color: white;
        text-align: center;
        white-space: nowrap;
        position: sticky;
        top: 0;
        z-index: 10;
        font-size: 11px;
        font-weight: 600;
        width: 20px;
    }
    
    .tracking-table td {
        padding: 2px 1px;
        border: 1px solid #dee2e6;
        text-align: center;
        font-size: 10px;
        width: 20px;
    }
    
    .tracking-table td.moc-col {
        font-weight: bold;
        background-color: #f8f9fa;
        color: #2c3e50;
        font-size: 11px;
        padding: 2px 1px;
        width: 25px;
    }
    
    .cell-hit {
        background-color: #28a745 !important;
        color: white;
        font-weight: bold;
        font-size: 16px;
    }
    
    .cell-miss {
        background-color: #dc3545 !important;
        color: white;
        font-size: 14px;
    }
    
    .day-header {
        background-color: #17a2b8;
        color: white;
        padding: 8px;
        border-radius: 4px;
        margin: 15px 0 5px 0;
        font-weight: 600;
        text-align: center;
    }
    
    /* Mobile optimization */
    @media (max-width: 768px) {
        .tracking-table {
            font-size: 9px;
            max-width: 100%;
        }
        .tracking-table th {
            padding: 4px 2px;
            font-size: 9px;
        }
        .tracking-table td {
            padding: 6px 2px;
            font-size: 11px;
        }
        .tracking-table td.moc-col {
            font-size: 12px;
        }
        .cell-hit {
            font-size: 13px;
        }
        .cell-miss {
            font-size: 11px;
        }
        .day-header {
            font-size: 11px;
            padding: 6px;
        }
        [data-testid="column"] {
            padding: 0 0.1rem !important;
        }
    }
</style>
""", unsafe_allow_html=True)

# --- QUẢN LÝ DỮ LIỆU ---
@st.cache_data(ttl=1800)
def get_master_data(num_days):
    # Tải song song tất cả các nguồn
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_dt = executor.submit(data_fetcher.fetch_dien_toan, num_days)
        f_tt = executor.submit(data_fetcher.fetch_than_tai, num_days)
        f_mb = executor.submit(data_fetcher.fetch_xsmb_full, num_days)
        
        dt = f_dt.result()
        tt = f_tt.result()
        mb_full = f_mb.result()  # Full XSMB data with all prizes

    # Xử lý khớp ngày (Quan trọng để không bị lệch)
    df_dt = pd.DataFrame(dt)
    df_tt = pd.DataFrame(tt)
    df_mb = pd.DataFrame(mb_full)  # XSMB DataFrame with all prizes

    # Gộp thành bảng tổng (Master Table)
    if not df_dt.empty and not df_mb.empty:
        df = pd.merge(df_dt, df_tt, on="date", how="left")
        df = pd.merge(df, df_mb, on="date", how="left")
        return df
    return pd.DataFrame()

# --- SIDEBAR ---
with st.sidebar:
    st.title("🐔 SIÊU GÀ TOOL")
    st.caption("Version: Matrix View")
    days_fetch = st.number_input("Số ngày tải:", 30, 365, 60, step=10)
    days_show = st.slider("Hiển thị:", 10, 100, 20)
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

df_show = df_full.head(days_show).copy()

# === 🎯 DÀN NUÔI (MATRIX) ===
st.title("🎯 DÀN NUÔI (MATRIX)")
st.divider()

# Row 1: Nguồn và Miền
c1, c2 = st.columns([1, 1])
src_mode = c1.selectbox("Nguồn:", ["Thần Tài", "Điện Toán", "Tự nhập"])
region = c2.selectbox("Miền:", ["Miền Bắc", "Miền Nam", "Miền Trung"])

# Input Tự nhập (nếu chọn)
manual_input = ""
if src_mode == "Tự nhập":
    manual_input = st.text_input("Nhập số nuôi (cách nhau bởi dấu phẩy hoặc khoảng trắng):", "00, 11, 22, 33, 44")

# Row 2: Cấu hình chi tiết
c3, c4, c5, c6, c7 = st.columns([1, 1.2, 0.8, 1, 1])

# Biến cấu hình
selected_day = "Tất cả"
selected_station = "Tất cả"
col_comp = ""

if region == "Miền Bắc":
    c3.selectbox("Thứ:", ["Tất cả"], disabled=True)
    c4.selectbox("Đài:", ["XSMB"], disabled=True)
    
    prize_mode = c5.selectbox("Giải:", ["Đặc Biệt", "Giải Nhất", "G6-G7"])
    
    # Map giải sang cột dữ liệu
    if prize_mode == "Đặc Biệt":
        col_comp = "db_2so"
    elif prize_mode == "Giải Nhất":
        col_comp = "g1_2so"
    else:
        col_comp = "g67_2so"
        
    selected_station = "XSMB"

else: # Miền Nam / Trung
    # Dropdown Thứ
    weekdays = ["Tất cả", "Chủ Nhật", "Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7"]
    selected_day = c3.selectbox("Thứ:", weekdays)
    
    # Dropdown Đài (dựa trên Miền và Thứ)
    if selected_day == "Tất cả":
        selected_station = "Tất cả"
        c4.selectbox("Đài:", ["Tất cả"], disabled=True)
    else:
        stations = data_fetcher.get_stations_by_day(region, selected_day)
        if not stations:
            st.error(f"⚠️ Không có đài nào mở thưởng vào {selected_day} ở {region}")
            st.stop()
        
        station_options = ["Tất cả"] + stations
        selected_station = c4.selectbox("Đài:", station_options)
    
    # Dropdown Giải
    prize_mode = c5.selectbox("Giải:", ["G6-7-8"])
    col_comp = "g678_2so"

# Khung nuôi và Backtest
check_range = c6.slider("Khung:", 1, 20, 7)
backtest_mode = c7.selectbox("Backtest:", ["Hiện tại", "Lùi 1", "Lùi 2", "Lùi 3", "Lùi 4", "Lùi 5"])

# Xác định cột so sánh (cho Miền Nam/Trung dùng G6-7-8)
# col_comp đã được set ở trên

# Tự động phân tích
backtest_offset = 0
if backtest_mode != "Hiện tại":
    backtest_offset = int(backtest_mode.split()[1])

if backtest_offset > 0:
    st.info(f"🔍 Backtest: Từ {backtest_offset} ngày trước")

# === LOAD DỮ LIỆU ===
df_display = None
df_check_source = None

if region == "Miền Bắc":
    # Sử dụng dữ liệu df_full đã load sẵn từ trước
    df_display = df_full
    df_check_source = df_full
else:
    # Load dữ liệu từ API
    if selected_station == "Tất cả":
        # Load tất cả các đài trong MIỀN (để có full data cho check liên tục)
        all_stations = data_fetcher.get_all_stations_in_region(region)
        
        with st.spinner(f"🔄 Đang tải dữ liệu toàn bộ {region} ({len(all_stations)} đài)..."):
            all_station_data = []
            # Tải song song
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future_to_station = {executor.submit(data_fetcher.fetch_station_data, s, days_fetch): s for s in all_stations}
                for future in concurrent.futures.as_completed(future_to_station):
                    station_name = future_to_station[future]
                    try:
                        data = future.result()
                        # Thêm tên đài vào mỗi dòng dữ liệu
                        for item in data:
                            item['station'] = station_name
                        all_station_data.extend(data)
                    except Exception as exc:
                        st.error(f"Lỗi tải đài {station_name}: {exc}")
            
            if not all_station_data:
                st.error("⚠️ Không thể tải dữ liệu")
                st.stop()
            
            # Chuyển đổi sang DataFrame và gom nhóm theo ngày
            df_temp = pd.DataFrame(all_station_data)
            
            # Gom nhóm theo ngày (Master Data cho Verification)
            grouped_data = []
            for date, group in df_temp.groupby('date'):
                day_results = []
                for _, row in group.iterrows():
                    # Lấy danh sách số từ G6-7-8
                    vals = row.get(col_comp, [])
                    if vals and isinstance(vals, list):
                        for val in vals:
                            if val:
                                day_results.append({'station': row['station'], 'val': val})
                
                if day_results:
                    grouped_data.append({'date': date, 'results': day_results})
            
            df_check_source = pd.DataFrame(grouped_data)
            
            # QUAN TRỌNG: Chuyển date string sang datetime để sort đúng
            df_check_source['date_obj'] = pd.to_datetime(df_check_source['date'], format='%d/%m/%Y')
            df_check_source = df_check_source.sort_values('date_obj', ascending=False)
            df_check_source = df_check_source.drop(columns=['date_obj'])  # Xóa cột tạm
            
            # Debug: Show data info
            with st.expander("🐞 Thông tin dữ liệu"):
                st.write(f"**Số đài đã tải**: {len(all_stations)}")
                st.write(f"**Tổng số bản ghi**: {len(all_station_data)}")
                st.write(f"**Số ngày có dữ liệu**: {len(df_check_source)}")
                if not df_check_source.empty:
                    st.write(f"**Ngày mới nhất**: {df_check_source.iloc[0]['date']}")
                    st.write(f"**Ngày cũ nhất**: {df_check_source.iloc[-1]['date']}")
            
            # Filter cho hiển thị (chỉ lấy những ngày đúng Thứ đã chọn)
            if selected_day == "Tất cả":
                # Hiển thị tất cả các ngày
                df_display = df_check_source.copy()
            else:
                WEEKDAY_MAP = {
                    "Thứ 2": 0, "Thứ 3": 1, "Thứ 4": 2, "Thứ 5": 3, "Thứ 6": 4, "Thứ 7": 5, "Chủ Nhật": 6
                }
                target_weekday = WEEKDAY_MAP.get(selected_day)
                
                def is_target_day(date_str):
                    try:
                        return datetime.strptime(date_str, "%d/%m/%Y").weekday() == target_weekday
                    except:
                        return False
                
                df_display = df_check_source[df_check_source['date'].apply(is_target_day)].copy()
            
    else:
        # Load dữ liệu cho đài đã chọn
        with st.spinner(f"🔄 Đang tải dữ liệu {selected_station}..."):
            station_data = data_fetcher.fetch_station_data(selected_station, total_days=days_fetch)
            
            if not station_data:
                st.error(f"⚠️ Không thể tải dữ liệu cho {selected_station}")
                st.stop()
            
            # Chuyển đổi sang DataFrame
            df_temp = pd.DataFrame(station_data)
            
            # Tạo results từ g678_2so (danh sách)
            def create_results(row):
                vals = row.get(col_comp, [])
                if vals and isinstance(vals, list):
                    return [{'station': selected_station, 'val': v} for v in vals if v]
                return []
            
            df_temp['results'] = df_temp.apply(create_results, axis=1)
            df_display = df_temp[['date', 'results']]
            df_check_source = df_display # Với 1 đài thì nguồn check cũng là chính nó

# Gán lại vào df_region để tương thích code phía dưới (df_region đóng vai trò là df_display)
df_region = df_display


all_days_data = []
start_idx = backtest_offset
end_idx = min(backtest_offset + 20, len(df_region))

# Tạo lookup dictionary cho df_full để tra cứu nhanh theo ngày
df_full_lookup = df_full.set_index('date') if not df_full.empty else pd.DataFrame()

for i in range(start_idx, end_idx):
    row = df_region.iloc[i]
    date_val = row['date']
    
    # Xác định dòng dữ liệu nguồn (Source Row)
    # Nếu là Miền Bắc thì chính là row hiện tại
    # Nếu là Miền Nam/Trung thì phải tìm ngày tương ứng trong df_full
    row_src = None
    if region == "Miền Bắc":
        row_src = row
    else:
        if date_val in df_full_lookup.index:
            row_src = df_full_lookup.loc[date_val]
            # Xử lý trường hợp trùng ngày (nếu có)
            if isinstance(row_src, pd.DataFrame):
                row_src = row_src.iloc[0]
    
    if row_src is None:
        continue

    src_str = ""
    combos = []
    
    if src_mode == "Tự nhập":
        src_str = manual_input
        if src_str:
            # Parse manual input
            raw_nums = [x.strip() for x in src_str.replace(',', ' ').split()]
            # Filter valid 2-digit numbers
            valid_nums = [n for n in raw_nums if n.isdigit() and len(n) == 2]
            combos = sorted(list(set(valid_nums)))
    else:
        if src_mode == "Thần Tài": 
            src_str = str(row_src.get('tt_number', ''))
        elif src_mode == "Điện Toán": 
            val = row_src.get('dt_numbers', [])
            if isinstance(val, list):
                 src_str = "".join(val)
            else:
                 src_str = str(val) if pd.notna(val) else ""
        
        if not src_str or src_str == "nan": 
            continue
        
        # Tách liên tiếp 2 vị trí thành 1 số và có đảo
        # VD: 1234 → 12, 23, 34 + đảo: 21, 32, 43
        combos_set = set()
        for i in range(len(src_str) - 1):
            pair = src_str[i:i+2]
            combos_set.add(pair)  # Số thuận
            combos_set.add(pair[::-1])  # Số đảo
        combos = sorted(combos_set)
    
    if not combos:
        continue
    
    # Store results for this date (for comparison later)
    # If Miền Bắc: result is in row[col_comp]
    # If Miền Nam/Trung: result is in row['results'] (list of dicts)
    
    date_results = []
    if region == "Miền Bắc":
        # Check if col_comp is a list-based column (like g67_2so)
        val_or_list = row.get(col_comp, "")
        if isinstance(val_or_list, list):
            # It's a list (G6-G7)
            for val in val_or_list:
                if val:
                    date_results.append({'station': 'XSMB', 'val': val})
        else:
            # It's a single value (ĐB or G1)
            val = str(val_or_list)
            if val and val != "nan":
                date_results.append({'station': 'XSMB', 'val': val})
    else:
        # row['results'] is already a list of dicts {station, val}
        res_list = row.get('results', [])
        if isinstance(res_list, list):
            date_results = res_list
            
    all_days_data.append({
        'date': row['date'], 
        'source': src_str, 
        'combos': combos, 
        'index': i,
        'results': date_results
    })

if not all_days_data:
    st.warning("⚠️ Không có dữ liệu")
else:
    st.markdown("### 📋 Bảng Theo Dõi")
    
    # Giới hạn số cột tối đa để tránh vỡ khung trên mobile
    MAX_COLS = 10
    
    # Lookup for verification
    check_source_lookup = df_check_source.set_index('date') if df_check_source is not None and not df_check_source.empty else pd.DataFrame()
    
    # Tạo bảng HTML dạng tam giác
    table_html = "<div class='table-wrapper'>"
    table_html += "<table class='tracking-table'><thead><tr>"
    table_html += "<th>Ngày</th>"
    table_html += "<th>Mốc</th>"
    
    # Header columns N1, N2, ... N10
    for k in range(1, MAX_COLS + 1):
        table_html += f"<th>N{k}</th>"
    table_html += "</tr></thead><tbody>"
    
    # Mỗi dòng = 1 dàn (1 ngày)
    for row_idx, day_data in enumerate(all_days_data):
        date, source, combos, i = day_data['date'], day_data['source'], day_data['combos'], day_data['index']
        
        table_html += "<tr>"
        # Cột Ngày
        table_html += f"<td style='font-size:8px;color:#495057;width:45px;padding:2px 1px;'>{date}</td>"
        # Cột Mốc: hiển thị số giải
        table_html += f"<td class='moc-col'>{source}</td>"
        
        # Số cột thực tế cho dòng này (dạng tam giác)
        num_cols_this_row = min(row_idx + 1, MAX_COLS)
        
        # Check từng cột N1, N2, ...
        for k in range(1, MAX_COLS + 1):
            if k > num_cols_this_row:
                # Ô trống (ngoài tam giác)
                table_html += "<td style='background-color:#f8f9fa;border:none;'></td>"
            else:
                check_results = []
                
                if selected_station == "Tất cả" and region != "Miền Bắc":
                    # Continuous Check: Date + k days
                    try:
                        current_date_obj = datetime.strptime(date, "%d/%m/%Y")
                        check_date_obj = current_date_obj + timedelta(days=k)
                        check_date_str = check_date_obj.strftime("%d/%m/%Y")
                        
                        if check_date_str in check_source_lookup.index:
                            check_row = check_source_lookup.loc[check_date_str]
                            if isinstance(check_row, pd.DataFrame):
                                check_row = check_row.iloc[0]
                                
                            res_list = check_row.get('results', [])
                            if isinstance(res_list, list):
                                check_results = res_list
                    except:
                        pass
                else:
                    # Index-based check (Next Draw)
                    check_idx = i - k
                    if check_idx >= 0 and check_idx < len(df_region):
                        check_row = df_region.iloc[check_idx]
                        
                        if region == "Miền Bắc":
                            # Check if col_comp is a list-based column
                            val_or_list = check_row.get(col_comp, "")
                            if isinstance(val_or_list, list):
                                for val in val_or_list:
                                    if val:
                                        check_results.append({'station': 'XSMB', 'val': val})
                            else:
                                val = str(val_or_list)
                                if val and val != "nan":
                                    check_results.append({'station': 'XSMB', 'val': val})
                        else:
                            res_list = check_row.get('results', [])
                            if isinstance(res_list, list):
                                check_results = res_list
                
                # Kiểm tra xem CÓ SỐ NÀO trong dàn này trúng không
                is_hit = False
                for res in check_results:
                    if res['val'] in combos:
                        is_hit = True
                        break
                
                # Render cell
                if is_hit:
                    table_html += "<td class='cell-hit'>✓</td>"
                elif check_results:  # Có dữ liệu nhưng không trúng
                    table_html += "<td class='cell-miss'>−</td>"
                else:  # Không có dữ liệu
                    table_html += "<td>−</td>"
        
        table_html += "</tr>"
    
    table_html += "</tbody></table></div>"
    st.markdown(table_html, unsafe_allow_html=True)
    
    # Divider sau bảng
    st.markdown("---")
    st.subheader("📊 Thống kê")
    total_days, total_checks, total_hits = len(all_days_data), 0, 0
    for row_idx, day_data in enumerate(all_days_data):
        combos, i, date = day_data['combos'], day_data['index'], day_data['date']
        for k in range(1, row_idx + 2):
            is_valid_check = False
            check_results = []
            
            if selected_station == "Tất cả" and region != "Miền Bắc":
                try:
                    current_date_obj = datetime.strptime(date, "%d/%m/%Y")
                    check_date_obj = current_date_obj + timedelta(days=k)
                    check_date_str = check_date_obj.strftime("%d/%m/%Y")
                    
                    if check_date_str in check_source_lookup.index:
                         check_row = check_source_lookup.loc[check_date_str]
                         if isinstance(check_row, pd.DataFrame): check_row = check_row.iloc[0]
                         res_list = check_row.get('results', [])
                         if isinstance(res_list, list):
                             check_results = res_list
                         is_valid_check = True
                except:
                    pass
            else:
                idx = i - k
                if idx >= 0 and idx >= backtest_offset:
                    is_valid_check = True
                    check_row = df_region.iloc[idx]
                    if region == "Miền Bắc":
                        val_or_list = check_row.get(col_comp, "")
                        if isinstance(val_or_list, list):
                            for val in val_or_list:
                                if val:
                                    check_results.append({'station': 'XSMB', 'val': val})
                        else:
                            val = str(val_or_list)
                            if val and val != "nan":
                                check_results.append({'station': 'XSMB', 'val': val})
                    else:
                        res_list = check_row.get('results', [])
                        if isinstance(res_list, list):
                            check_results = res_list

            if is_valid_check:
                total_checks += 1
                is_hit = False
                for res in check_results:
                    if res['val'] in combos:
                        is_hit = True
                        break
                if is_hit:
                    total_hits += 1
    
    hit_rate = round(total_hits / total_checks * 100, 1) if total_checks > 0 else 0
    col_s1, col_s2, col_s3, col_s4 = st.columns(4)
    col_s1.metric("Tổng ngày", total_days)
    col_s2.metric("Tổng kiểm tra", total_checks)
    col_s3.metric("Đã trúng", total_hits)
    col_s4.metric("Tỷ lệ", f"{hit_rate}%")
    
    # === TỔNG HỢP DÀN CHƯA RA ===
    st.markdown("---")
    st.subheader("🎯 Tổng hợp Dàn Chưa Ra")
    
    pending_by_date = []
    
    for row_idx, day_data in enumerate(all_days_data):
        combos = day_data['combos']
        date = day_data['date']
        i = day_data['index']
        num_cols_this_row = row_idx + 1
        hit_numbers = set()
        
        # Kiểm tra xem có số nào trong dàn đã trúng chưa (chỉ xét dữ liệu lịch sử)
        for k in range(1, num_cols_this_row + 1):
            check_results = []
            
            if selected_station == "Tất cả" and region != "Miền Bắc":
                try:
                    current_date_obj = datetime.strptime(date, "%d/%m/%Y")
                    check_date_obj = current_date_obj + timedelta(days=k)
                    check_date_str = check_date_obj.strftime("%d/%m/%Y")
                    
                    if check_date_str in check_source_lookup.index:
                         check_row = check_source_lookup.loc[check_date_str]
                         if isinstance(check_row, pd.DataFrame): check_row = check_row.iloc[0]
                         res_list = check_row.get('results', [])
                         if isinstance(res_list, list):
                             check_results = res_list
                except:
                            check_results = res_list
            
            for res in check_results:
                if res['val'] in combos:
                    hit_numbers.add(res['val'])
        
        # Nếu CHƯA có số nào trúng (hit_numbers rỗng) thì dàn này chưa ra
        if not hit_numbers:
            try:
                date_obj = datetime.strptime(date, "%d/%m/%Y")
                weekday_names = ["Thứ 2", "Thứ 3", "Thứ 4", "Thứ 5", "Thứ 6", "Thứ 7", "Chủ Nhật"]
                weekday = weekday_names[date_obj.weekday()]
            except:
                weekday = ""
            
            pending_by_date.append({
                'Ngày': f"{weekday} {date}" if weekday else date,
                'Dàn liên tiếp': ', '.join(sorted(combos)),
                'Số lượng': len(combos),
                'combos': combos  # Giữ lại để phân tích tần suất
            })
    
    if pending_by_date:
        # Hiển thị bảng theo ngày
        df_display = pd.DataFrame([{k: v for k, v in item.items() if k != 'combos'} for item in pending_by_date])
        st.dataframe(df_display, use_container_width=True, hide_index=True)
        
        # Phân tích tần suất các số trong các dàn chưa ra
        st.markdown("---")
        st.markdown("**📊 Mức số trong các dàn chưa ra:**")
        st.caption("Đếm số lần xuất hiện của mỗi số trong tất cả các dàn chưa ra")
        
        # Đếm tần suất
        from collections import defaultdict
        number_frequency = defaultdict(int)
        for item in pending_by_date:
            for num in item['combos']:
                number_frequency[num] += 1
        
        # Nhóm theo mức (bao gồm mức 0)
        level_groups = defaultdict(list)
        for num, freq in number_frequency.items():
            level_groups[freq].append(num)
        
        # Tìm tất cả số từ 00-99 và thêm mức 0
        all_possible_numbers = {f"{i:02d}" for i in range(100)}
        numbers_in_pending = set(number_frequency.keys())
        level_0_numbers = sorted(all_possible_numbers - numbers_in_pending)
        
        if level_0_numbers:
            level_groups[0] = level_0_numbers
        
        # Hiển thị theo mức giảm dần
        for freq in sorted(level_groups.keys(), reverse=True):
            nums = sorted(level_groups[freq])
            st.write(f"**Mức {freq}** ({len(nums)} số): {', '.join(nums)}")
        
        # Thống kê tổng quan
        st.markdown("---")
        total_days_pending = len(pending_by_date)
        total_unique_numbers = len(number_frequency)
        col_p1, col_p2 = st.columns(2)
        col_p1.metric("Số ngày có dàn chưa ra", total_days_pending)
        col_p2.metric("Tổng số unique trong các dàn", total_unique_numbers)
    else:
        st.success("✅ Tất cả các dàn đều đã ra (có ít nhất 1 số trúng)!")
    
    # === PHÂN TÍCH CHU KỲ & NHẬN ĐỊNH ===
    st.markdown("---")
    st.subheader("🔮 Phân tích Chu kỳ & Nhận định")
    st.caption("Dựa trên dữ liệu bảng theo dõi")
    
    # Thu thập dữ liệu chu kỳ cho mỗi dàn
    cycle_analysis = []
    
    for row_idx, day_data in enumerate(all_days_data):
        combos = day_data['combos']
        date = day_data['date']
        i = day_data['index']
        
        # Phân tích dữ liệu từ bảng theo dõi
        num_cols_this_row = row_idx + 1
        hits = []  # Vị trí các lần trúng (1, 2, 3...)
        misses = []  # Vị trí các lần không trúng
        
        for k in range(1, num_cols_this_row + 1):
            check_results = []
            
            if selected_station == "Tất cả" and region != "Miền Bắc":
                # Use date-based check for All Stations
                try:
                    current_date_obj = datetime.strptime(date, "%d/%m/%Y")
                    check_date_obj = current_date_obj + timedelta(days=k)
                    check_date_str = check_date_obj.strftime("%d/%m/%Y")
                    
                    if check_date_str in check_source_lookup.index:
                         check_row = check_source_lookup.loc[check_date_str]
                         if isinstance(check_row, pd.DataFrame): check_row = check_row.iloc[0]
                         res_list = check_row.get('results', [])
                         if isinstance(res_list, list):
                             check_results = res_list
                except:
                    pass
            else:
                # Use index-based check for single station/Miền Bắc
                idx = i - k
                if idx >= 0 and idx >= backtest_offset:
                    check_row = df_region.iloc[idx]
                    if region == "Miền Bắc":
                        val_or_list = check_row.get(col_comp, "")
                        if isinstance(val_or_list, list):
                            for val in val_or_list:
                                if val:
                                    check_results.append({'station': 'XSMB', 'val': val})
                        else:
                            val = str(val_or_list)
                            if val and val != "nan":
                                check_results.append({'station': 'XSMB', 'val': val})
                    else:
                        res_list = check_row.get('results', [])
                        if isinstance(res_list, list):
                            check_results = res_list
            
            # Check if any result matches
            is_hit = False
            for res in check_results:
                if res['val'] in combos:
                    is_hit = True
                    break
            
            if is_hit:
                hits.append(k)
            elif check_results:  # Only count as miss if there was data to check
                misses.append(k)
        
        # Tính toán chu kỳ và nhận định
        total_checks = len(hits) + len(misses)
        hit_count = len(hits)
        miss_count = len(misses)
        
        if total_checks == 0:
            status = "🆕 Mới tạo - Chưa có dữ liệu"
            avg_cycle_display = "N/A"
            last_hit_display = "N/A"
            priority = 2
            overdue = 0
        elif hit_count == 0:
            # Chưa ra lần nào
            status = f"🔥 Chưa ra ({total_checks} ngày kiểm tra) - Ưu tiên cao"
            avg_cycle_display = "Chưa ra"
            last_hit_display = "Chưa bao giờ"
            priority = 0
            overdue = total_checks
        else:
            # Đã ra ít nhất 1 lần
            # Tính chu kỳ giữa các lần trúng
            if len(hits) > 1:
                cycles = [hits[j-1] - hits[j] for j in range(1, len(hits))]
                avg_cycle = round(sum(cycles) / len(cycles), 1)
            else:
                avg_cycle = hits[0]
            
            avg_cycle_display = f"{avg_cycle} ngày"
            last_hit_display = f"N{hits[0]}"
            
            # Nhận định dựa trên chu kỳ
            days_since_last = hits[0] - 1  # Số ngày từ lần trúng cuối
            
            if days_since_last == 0:
                status = "✅ Vừa trúng hôm qua"
                priority = 2
                overdue = 0
            elif days_since_last < avg_cycle:
                remaining = round(avg_cycle - days_since_last)
                status = f"⏳ Trong chu kỳ (còn ~{remaining} ngày)"
                priority = 2
                overdue = 0
            else:
                overdue_days = days_since_last - avg_cycle
                if overdue_days > avg_cycle * 0.5:
                    status = f"⚠️ Quá chu kỳ {round(overdue_days)} ngày - Ưu tiên cao"
                    priority = 1
                    overdue = overdue_days
                else:
                    status = f"📍 Quá chu kỳ {round(overdue_days)} ngày"
                    priority = 1
                    overdue = overdue_days
        
        cycle_analysis.append({
            'Ngày': date,
            'Dàn': ', '.join(sorted(combos)),
            'Chu kỳ TB': avg_cycle_display,
            'Lần cuối ra': last_hit_display,
            'Đã kiểm tra': total_checks,
            'Trúng/Trượt': f"{hit_count}/{miss_count}",
            'Nhận định': status,
            # Thêm các trường ẩn để sắp xếp
            '_sort_priority': priority,
            '_overdue_days': overdue,
            '_total_checks': total_checks
        })
    
    if cycle_analysis:
        # Sắp xếp: Ưu tiên chưa ra (nhiều ngày nhất), sau đó quá chu kỳ nhiều nhất, sau đó trong chu kỳ
        cycle_analysis.sort(key=lambda x: (x['_sort_priority'], -x['_overdue_days'], -x['_total_checks']))
        
        # Loại bỏ các trường ẩn trước khi hiển thị
        cycle_analysis_display = [{k: v for k, v in item.items() if not k.startswith('_')} for item in cycle_analysis]
        
        df_cycle = pd.DataFrame(cycle_analysis_display)
        st.dataframe(df_cycle, use_container_width=True, hide_index=True)
        
        # Gợi ý ưu tiên
        st.markdown("---")
        st.markdown("**💡 Gợi ý ưu tiên theo dõi:**")
        
        # Lọc các dàn ưu tiên cao
        priority_sets = [item for item in cycle_analysis if "Ưu tiên cao" in item['Nhận định'] or "Chưa ra lần nào" in item['Nhận định']]
        
        if priority_sets:
            st.info(f"Có **{len(priority_sets)}** dàn cần ưu tiên theo dõi (quá hạn hoặc chưa ra lần nào)")
            
            # Hiển thị danh sách dàn ưu tiên
            st.markdown("**📋 Danh sách dàn ưu tiên:**")
            for idx, item in enumerate(priority_sets, 1):
                st.write(f"{idx}. **{item['Ngày']}**: {item['Dàn']} - _{item['Nhận định']}_")
            
            # Phân tích mức số trong các dàn ưu tiên
            st.markdown("---")
            st.markdown("**📊 Mức số trong các dàn ưu tiên:**")
            
            from collections import defaultdict
            priority_number_freq = defaultdict(int)
            
            # Đếm tần suất từ dàn gốc (không phải string đã format)
            for row_idx, day_data in enumerate(all_days_data):
                date = day_data['date']
                combos = day_data['combos']
                
                # Kiểm tra xem dàn này có trong danh sách ưu tiên không
                is_priority = any(p['Ngày'] == date for p in priority_sets)
                
                if is_priority:
                    for num in combos:
                            priority_number_freq[num] += 1
            
            # Nhóm theo mức (bao gồm mức 0)
            level_groups_priority = defaultdict(list)
            for num, freq in priority_number_freq.items():
                level_groups_priority[freq].append(num)
            
            # Tìm tất cả số từ 00-99 và thêm mức 0
            all_possible_numbers = {f"{i:02d}" for i in range(100)}
            numbers_in_priority = set(priority_number_freq.keys())
            level_0_numbers = sorted(all_possible_numbers - numbers_in_priority)
            
            if level_0_numbers:
                level_groups_priority[0] = level_0_numbers
            
            # Hiển thị theo mức giảm dần
            for freq in sorted(level_groups_priority.keys(), reverse=True):
                nums = sorted(level_groups_priority[freq])
                st.write(f"**Mức {freq}** ({len(nums)} số): {', '.join(nums)}")
        else:
            st.success("Tất cả các dàn đang trong chu kỳ bình thường")
    else:
        pass  # Không có dữ liệu để phân tích chu kỳ

