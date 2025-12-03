import requests
import json
import logging
import time
from bs4 import BeautifulSoup
from typing import List, Dict, Union

# Cấu hình Logging
logging.basicConfig(level=logging.INFO)

# Header giả lập trình duyệt để tránh bị chặn
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.kqxs88.live/"
}

# API Endpoint gốc lấy từ file HTML
API_BASE_URL = "https://www.kqxs88.live/api/front/open/lottery/history/list/game"

# BẢNG MÃ ĐÀI (Mapping Name -> Code)
# Dựa trên quy tắc đặt tên của kqxs88 (4 chữ cái đầu hoặc viết tắt)
STATION_CODES = {
    # --- MIỀN BẮC ---
    "Miền Bắc": "miba",
    
    # --- MIỀN NAM ---
    "TP. Hồ Chí Minh": "tphc", "Đồng Tháp": "doth", "Cà Mau": "cama",
    "Bến Tre": "betr", "Vũng Tàu": "vuta", "Bạc Liêu": "bali",
    "Đồng Nai": "dona", "Cần Thơ": "cath", "Sóc Trăng": "sotr",
    "Tây Ninh": "tani", "An Giang": "angi", "Bình Thuận": "bith",
    "Vĩnh Long": "vilo", "Bình Dương": "bidu", "Trà Vinh": "trvi",
    "Long An": "loan", "Bình Phước": "biph", "Hậu Giang": "hagi",
    "Tiền Giang": "tigi", "Kiên Giang": "kigi", "Đà Lạt": "dalat", 
    # Lưu ý: Lâm Đồng thường dùng mã 'dalat' hoặc 'lado', check file HTML dùng 'dalat' ở list tên
    
    # --- MIỀN TRUNG ---
    "Thừa Thiên Huế": "thth", "Phú Yên": "phye",
    "Đắk Lắk": "dalak", "Quảng Nam": "quna",
    "Đà Nẵng": "dana", "Khánh Hòa": "khho",
    "Bình Định": "bidi", "Quảng Trị": "qutr", "Quảng Bình": "qubi",
    "Gia Lai": "gila", "Ninh Thuận": "nith",
    "Quảng Ngãi": "qung", "Đắk Nông": "dano", "Kon Tum": "kotu"
}

# Lịch quay không thay đổi
LICH_QUAY_NAM = {
    "Chủ Nhật": ["Tiền Giang", "Kiên Giang", "Đà Lạt"],
    "Thứ 2": ["Đồng Tháp", "TP. Hồ Chí Minh", "Cà Mau"],
    "Thứ 3": ["Bến Tre", "Vũng Tàu", "Bạc Liêu"],
    "Thứ 4": ["Đồng Nai", "Cần Thơ", "Sóc Trăng"],
    "Thứ 5": ["Tây Ninh", "An Giang", "Bình Thuận"],
    "Thứ 6": ["Vĩnh Long", "Bình Dương", "Trà Vinh"],
    "Thứ 7": ["TP. Hồ Chí Minh", "Long An", "Bình Phước", "Hậu Giang"]
}

LICH_QUAY_TRUNG = {
    "Chủ Nhật": ["Kon Tum", "Khánh Hòa"],
    "Thứ 2": ["Thừa Thiên Huế", "Phú Yên"],
    "Thứ 3": ["Đắk Lắk", "Quảng Nam"],
    "Thứ 4": ["Đà Nẵng", "Khánh Hòa"],
    "Thứ 5": ["Bình Định", "Quảng Trị", "Quảng Bình"],
    "Thứ 6": ["Gia Lai", "Ninh Thuận"],
    "Thứ 7": ["Đà Nẵng", "Quảng Ngãi", "Đắk Nông"]
}

def get_stations_by_day(region: str, day: str) -> List[str]:
    """Trả về danh sách tên đài theo ngày và miền."""
    if region == "Miền Nam":
        return LICH_QUAY_NAM.get(day, [])
    elif region == "Miền Trung":
        return LICH_QUAY_TRUNG.get(day, [])
    return []

def get_all_stations_in_region(region: str) -> List[str]:
    """Trả về tất cả các đài trong miền."""
    stations = set()
    schedule = {}
    if region == "Miền Nam":
        schedule = LICH_QUAY_NAM
    elif region == "Miền Trung":
        schedule = LICH_QUAY_TRUNG
        
    for day_stations in schedule.values():
        stations.update(day_stations)
        
    return sorted(list(stations))

def _extract_tails(prize_content) -> List[str]:
    """
    Tách 2 số cuối từ nội dung giải.
    Input có thể là string ("1234,5678") hoặc int/string đơn ("12345").
    """
    results = []
    if not prize_content:
        return results
        
    # Chuyển về string nếu chưa phải
    s = str(prize_content)
    
    # Tách theo dấu phẩy (nếu có nhiều số trong 1 giải)
    parts = s.split(',')
    
    for part in parts:
        part = part.strip()
        if len(part) >= 2 and part.isdigit():
            results.append(part[-2:])
            
    return results

def fetch_station_data(station_name: str, total_days: int = 60) -> List[Dict]:
    """
    Hàm gọi API chính.
    Logic: Lấy code từ tên -> Gọi API -> Parse JSON detail -> Trả về list clean.
    """
    game_code = STATION_CODES.get(station_name)
    if not game_code:
        logging.error(f"Không tìm thấy mã game cho đài: {station_name}")
        return []

    params = {
        "limitNum": total_days,
        "gameCode": game_code
    }

    try:
        # Gọi API với params
        response = requests.get(API_BASE_URL, params=params, headers=HEADERS, timeout=15)
        response.raise_for_status()
        
        data_json = response.json()
        
        # Cấu trúc JSON thường là: { "data": { "issueList": [...] } } 
        # hoặc đôi khi { "t": { "issueList": [...] } } tùy server cache
        issue_list = []
        if "data" in data_json and data_json["data"]:
             issue_list = data_json["data"].get("issueList", [])
        elif "t" in data_json and data_json["t"]:
             issue_list = data_json["t"].get("issueList", [])
        elif "issueList" in data_json:
             issue_list = data_json["issueList"]
             
        results = []
        
        for issue in issue_list:
            turn_num = issue.get("turnNum", "") # Ngày quay (DD/MM/YYYY)
            detail_raw = issue.get("detail", "")
            
            if not turn_num or not detail_raw:
                continue

            # Parse trường 'detail' (thường là string JSON)
            prizes = []
            if isinstance(detail_raw, str):
                try:
                    prizes = json.loads(detail_raw)
                except json.JSONDecodeError:
                    continue
            elif isinstance(detail_raw, list):
                prizes = detail_raw
            else:
                continue
                
            # Đảm bảo prizes là list
            if not isinstance(prizes, list) or not prizes:
                continue

            # --- MAPPING GIẢI ---
            # MB: 0=ĐB, 1=G1 ... 7=G7 (G7 có 4 số)
            # MN/MT: 0=ĐB, 1=G1 ... 8=G8 (G8 có 1 số 2 chữ số)
            
            db_val = prizes[0] if len(prizes) > 0 else ""
            
            # Khởi tạo object kết quả
            res_obj = {
                "date": turn_num,
                "db": str(db_val),
                "db_2so": str(db_val)[-2:] if str(db_val) else "",
                "g6_list": [],
                "g7_list": [],
                "g8_list": [] # Chỉ dùng cho MN/MT
            }
            
            if game_code == "miba": # Miền Bắc
                # MB: G6 index 6, G7 index 7
                if len(prizes) > 6: res_obj["g6_list"] = _extract_tails(prizes[6])
                if len(prizes) > 7: res_obj["g7_list"] = _extract_tails(prizes[7])
                # MB ko có G8
            else: # Miền Nam / Trung
                # MN/MT: G6 index 6, G7 index 7, G8 index 8
                if len(prizes) > 6: res_obj["g6_list"] = _extract_tails(prizes[6])
                if len(prizes) > 7: res_obj["g7_list"] = _extract_tails(prizes[7])
                if len(prizes) > 8: res_obj["g8_list"] = _extract_tails(prizes[8])

            results.append(res_obj)
            
        return results

    except Exception as e:
        logging.error(f"Lỗi khi tải dữ liệu đài {station_name} ({game_code}): {e}")
        return []

# --- CÁC HÀM CRAWL PHỤ (Giữ nguyên hoặc tối ưu nhẹ) ---

def fetch_url(url: str, max_retries: int = 3) -> BeautifulSoup:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return None
    return None

def _normalize_date(date_str: str) -> str:
    try:
        if "ngày" in date_str:
            date_str = date_str.split("ngày")[-1].strip()
        return date_str.replace("-", "/")
    except:
        return date_str

def fetch_dien_toan(total_days: int) -> List[Dict]:
    # Crawl từ trang ketqua04 (hoặc nguồn khác nếu cần)
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-dien-toan-123/{total_days}")
    data = []
    if not soup: return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_123")
        for div in divs:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            
            date = _normalize_date(date_raw)
            tbl = div.find("table", id="result_tab_123")
            if tbl:
                row = tbl.find("tbody").find("tr")
                cells = row.find_all("td") if row else []
                if len(cells) == 3:
                    nums = [c.text.strip() for c in cells]
                    if all(n.isdigit() for n in nums):
                        data.append({"date": date, "dt_numbers": nums})
            if len(data) >= total_days: break
    except Exception as e:
        logging.error(f"Error parsing Điện Toán: {e}")
    return data

def fetch_than_tai(total_days: int) -> List[Dict]:
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-than-tai/{total_days}")
    data = []
    if not soup: return data
    
    try:
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            
            date = _normalize_date(date_raw)
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
            if len(data) >= total_days: break
    except Exception as e:
        logging.error(f"Error parsing Thần Tài: {e}")
    return data

def fetch_xsmb_full(total_days: int) -> List[Dict]:
    """Wrapper cho Miền Bắc sử dụng hàm chuẩn."""
    return fetch_station_data("Miền Bắc", total_days)
