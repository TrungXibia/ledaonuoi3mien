import requests
import concurrent.futures
from bs4 import BeautifulSoup
import logging
import time
from typing import List, Dict
import re

logging.basicConfig(level=logging.INFO)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
}

# === MAPPING TÊN ĐÀI SANG SLUG MINH NGỌC ===
STATION_SLUGS = {
    # Miền Bắc
    "Miền Bắc": "mien-bac",
    # Miền Nam
    "TP. Hồ Chí Minh": "tp-hcm",
    "Đồng Tháp": "dong-thap",
    "Cà Mau": "ca-mau",
    "Bến Tre": "ben-tre",
    "Vũng Tàu": "vung-tau",
    "Bạc Liêu": "bac-lieu",
    "Đồng Nai": "dong-nai",
    "Cần Thơ": "can-tho",
    "Sóc Trăng": "soc-trang",
    "Tây Ninh": "tay-ninh",
    "An Giang": "an-giang",
    "Bình Thuận": "binh-thuan",
    "Vĩnh Long": "vinh-long",
    "Bình Dương": "binh-duong",
    "Trà Vinh": "tra-vinh",
    "Long An": "long-an",
    "Bình Phước": "binh-phuoc",
    "Hậu Giang": "hau-giang",
    "Tiền Giang": "tien-giang",
    "Kiên Giang": "kien-giang",
    "Đà Lạt": "da-lat",
    # Miền Trung
    "Thừa Thiên Huế": "thua-thien-hue",
    "Phú Yên": "phu-yen",
    "Đắk Lắk": "dak-lak",
    "Quảng Nam": "quang-nam",
    "Đà Nẵng": "da-nang",
    "Khánh Hòa": "khanh-hoa",
    "Bình Định": "binh-dinh",
    "Quảng Trị": "quang-tri",
    "Quảng Bình": "quang-binh",
    "Gia Lai": "gia-lai",
    "Ninh Thuận": "ninh-thuan",
    "Quảng Ngãi": "quang-ngai",
    "Đắk Nông": "dak-nong",
    "Kon Tum": "kon-tum"
}

LICH_QUAY_NAM = {
    "Chủ Nhật": ["Tiền Giang", "Kiên Giang", "Đà Lạt"],
    "Thứ 2": ["Đồng Tháp", "TP. Hồ Chí Minh", "Cà Mau"],
    "Thứ 3": ["Bến Tre", "Vũng Tàu", "Bạc Liêu"],
    "Thứ 4": ["Đồng Nai", "Cần Thơ", "Sóc Trăng"],
    "Thứ 5": ["Tây Ninh", "An Giang", "Bình Thuận"],
    "Thứ 6": ["Trà Vinh", "Vĩnh Long", "Bình Dương"],
    "Thứ 7": ["Long An", "Bình Phước", "Hậu Giang", "TP. Hồ Chí Minh"]
}

LICH_QUAY_TRUNG = {
    "Chủ Nhật": ["Khánh Hòa", "Kon Tum"],
    "Thứ 2": ["Thừa Thiên Huế", "Phú Yên"],
    "Thứ 3": ["Đắk Lắk", "Quảng Nam"],
    "Thứ 4": ["Khánh Hòa", "Đà Nẵng"],
    "Thứ 5": ["Quảng Trị", "Bình Định", "Quảng Bình"],
    "Thứ 6": ["Gia Lai", "Ninh Thuận"],
    "Thứ 7": ["Quảng Ngãi", "Đà Nẵng", "Đắk Nông"]
}

def get_stations_by_day(region: str, day: str) -> List[str]:
    if region == "Miền Nam":
        return LICH_QUAY_NAM.get(day, [])
    elif region == "Miền Trung":
        return LICH_QUAY_TRUNG.get(day, [])
    return []

def get_all_stations_in_region(region: str) -> List[str]:
    stations = set()
    schedule = {}
    if region == "Miền Nam":
        schedule = LICH_QUAY_NAM
    elif region == "Miền Trung":
        schedule = LICH_QUAY_TRUNG
        
    for day_stations in schedule.values():
        stations.update(day_stations)
        
    return sorted(list(stations))

def _normalize_date(date_str: str) -> str:
    """Chuyển đổi các định dạng ngày về dd/mm/yyyy"""
    if not date_str: return ""
    try:
        # Xử lý format Minh Ngọc: "Thứ Hai ngày 17/02/2024" hoặc "17-02-2024"
        match = re.search(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", date_str)
        if match:
            return f"{int(match.group(1)):02d}/{int(match.group(2)):02d}/{match.group(3)}"
        return date_str
    except:
        return date_str

def _extract_numbers(cell_content) -> List[str]:
    """Lấy các số từ một ô giải (xử lý <br>, -, ...)"""
    if not cell_content: return []
    # Loại bỏ các tag HTML không cần thiết
    text = cell_content.get_text(separator=" ", strip=True)
    # Tìm tất cả chuỗi số
    nums = re.findall(r'\d+', text)
    # Lọc các số có ý nghĩa (độ dài >= 2)
    return [n for n in nums if len(n) >= 2]

def _extract_tails(numbers: List[str]) -> List[str]:
    """Lấy 2 số cuối từ danh sách các số"""
    return [n[-2:] for n in numbers]

def fetch_url(url: str, max_retries: int = 3) -> BeautifulSoup:
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.RequestException:
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return None
    return None

def parse_minhngoc_page(soup: BeautifulSoup, station_name: str, existing_dates: set) -> List[Dict]:
    results = []
    # Tìm bảng kết quả. Minh Ngọc dùng class 'box_kqxs' cho mỗi ngày
    tables = soup.find_all("div", class_="box_kqxs")
    if not tables:
        # Fallback cho structure khác (mobile hoặc view theo tỉnh)
        tables = soup.find_all("table", class_="bkqtinhmiot")

    for table in tables:
        try:
            # Lấy ngày
            date_div = table.find("div", class_="title") or table.find("td", class_="ngay")
            if not date_div: continue
            
            date_str = _normalize_date(date_div.text)
            if not date_str or date_str in existing_dates:
                continue

            existing_dates.add(date_str)
            
            # Helper lấy giải
            def get_prize(class_name):
                td = table.find("td", class_=class_name)
                return _extract_numbers(td)

            db_nums = get_prize("giaidb")
            db_str = db_nums[0] if db_nums else ""
            
            # Cấu trúc dữ liệu trả về
            result = {
                "date": date_str,
                "db": db_str,
                "db_2so": db_str[-2:] if db_str else "",
                "station": station_name
            }

            # Lấy giải 1 để hiển thị (tùy chọn)
            g1_nums = get_prize("giai1")
            result["g1_2so"] = g1_nums[0][-2:] if g1_nums else ""

            # Lấy list số cho các giải quan trọng (để tính toán)
            # Miền Bắc: G1-G7. Miền Nam/Trung: G1-G8.
            
            if station_name == "Miền Bắc":
                # MB có giải 6 (3 số), giải 7 (4 số)
                result["g6_list"] = _extract_tails(get_prize("giai6"))
                result["g7_list"] = _extract_tails(get_prize("giai7"))
                result["g8_list"] = [] # MB không có G8
            else:
                # MN/MT
                result["g6_list"] = _extract_tails(get_prize("giai6"))
                result["g7_list"] = _extract_tails(get_prize("giai7"))
                result["g8_list"] = _extract_tails(get_prize("giai8"))
            
            results.append(result)
            
        except Exception as e:
            logging.error(f"Error parsing table for {station_name}: {e}")
            continue
            
    return results

def fetch_station_data(station_name: str, total_days: int = 60) -> List[Dict]:
    slug = STATION_SLUGS.get(station_name)
    if not slug:
        logging.error(f"No slug found for station: {station_name}")
        return []

    # Minh Ngọc phân trang: /ket-qua-xo-so/{slug}/page-{n}.html
    # Một trang thường có khoảng 10-20 kết quả. Fetch khoảng 4-5 trang để chắc chắn đủ 60 ngày.
    
    base_url = f"https://www.minhngoc.net.vn/ket-qua-xo-so/{slug}"
    all_results = []
    existing_dates = set()
    
    pages_to_fetch = (total_days // 15) + 2 # Ước lượng số trang cần thiết
    
    for page in range(1, pages_to_fetch + 1):
        if len(all_results) >= total_days:
            break
            
        url = f"{base_url}/page-{page}.html" if page > 1 else f"{base_url}.html"
        
        soup = fetch_url(url)
        if not soup: break
        
        page_results = parse_minhngoc_page(soup, station_name, existing_dates)
        if not page_results: break # Hết dữ liệu
        
        all_results.extend(page_results)
    
    # Sắp xếp theo ngày giảm dần (mới nhất trước)
    all_results.sort(key=lambda x:  datetime_from_string(x['date']), reverse=True)
    return all_results[:total_days]

def datetime_from_string(d_str):
    try:
        return time.strptime(d_str, "%d/%m/%Y")
    except:
        return time.struct_time((2000, 1, 1, 0, 0, 0, 0, 0, 0))

# === FETCH CÁC LOẠI KHÁC (ĐIỆN TOÁN, THẦN TÀI) ===
# Sử dụng Minh Ngọc cho ổn định thay vì ketqua04
def fetch_dien_toan(total_days: int) -> List[Dict]:
    # Điện toán 123
    data = []
    # Minh Ngọc URL cho 123: https://www.minhngoc.net.vn/xo-so-dien-toan/123.html
    # Lưu ý: Cấu trúc trang điện toán hơi khác, cần parser riêng hoặc fallback về ketqua.net nếu cần
    # Để đơn giản và nhanh, ta dùng ketqua.net (nguồn cũ nhưng sửa link chuẩn)
    # Link chuẩn hiện tại: https://ketqua.net/xo-so-dien-toan-123.php (nhưng khó parse lịch sử)
    
    # Ta dùng lại logic cũ nhưng trỏ về ketqua04 (hoặc ketqua.net) nếu còn sống.
    # Nếu sieuga79 dùng ketqua.net, ta thử fetch lại từ nguồn cũ với URL sửa đổi
    
    soup = fetch_url(f"https://ketqua.net/so-ket-qua-dien-toan-123/{total_days}") # Thử ketqua.net
    if not soup: return []
    
    try:
        divs = soup.find_all("div", class_="result_div")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            
            date = _normalize_date(date_raw)
            # Tìm bảng kết quả
            tables = div.find_all("table")
            for tbl in tables:
                if "ketqua_tbl" in str(tbl.get("class", [])) or "table" in str(tbl.name):
                    # Logic parse tùy thuộc vào HTML cụ thể của ketqua.net
                    # Fallback đơn giản: tìm các số
                    tds = tbl.find_all("td")
                    nums = [td.text.strip() for td in tds if td.text.strip().isdigit()]
                    if len(nums) >= 3:
                        # Điện toán 123 thường có 3 bộ số
                        data.append({"date": date, "dt_numbers": nums[:3]})
                        break
    except Exception as e:
        logging.error(f"Error parsing DT123: {e}")
    return data

def fetch_than_tai(total_days: int) -> List[Dict]:
    # Thần tài 4 (ketqua.net/so-ket-qua-than-tai)
    soup = fetch_url(f"https://ketqua.net/so-ket-qua-than-tai/{total_days}")
    data = []
    if not soup: return data
    
    try:
        divs = soup.find_all("div", class_="result_div")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            if not date_raw: continue
            
            date = _normalize_date(date_raw)
            # Thần tài 4 số
            tds = div.find_all("td")
            found = False
            for td in tds:
                txt = td.text.strip()
                if txt.isdigit() and len(txt) == 4:
                    data.append({"date": date, "tt_number": txt})
                    found = True
                    break
            if not found:
                 # Thử tìm input value hoặc structure khác nếu cần
                 pass
    except Exception as e:
        logging.error(f"Error parsing Than Tai: {e}")
    return data

def fetch_xsmb_full(total_days: int) -> List[Dict]:
    """Wrapper cho Miền Bắc dùng hàm chung"""
    return fetch_station_data("Miền Bắc", total_days)
