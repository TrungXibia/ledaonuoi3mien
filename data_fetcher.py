import requests
import concurrent.futures
from bs4 import BeautifulSoup
import logging
import time
from typing import List, Dict, Tuple
import json
import re

logging.basicConfig(level=logging.INFO)
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}

# === MIỀN NAM & MIỀN TRUNG DATA ===
DAI_API = {
    "An Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=angi",
    "Bạc Liêu": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bali",
    "Bến Tre": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=betr",
    "Bình Dương": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bidu",
    "Bình Thuận": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bith",
    "Bình Phước": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=biph",
    "Cà Mau": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=cama",
    "Cần Thơ": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=cath",
    "Đà Lạt": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dalat",
    "Đồng Nai": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dona",
    "Đồng Tháp": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=doth",
    "Hậu Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=hagi",
    "Kiên Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=kigi",
    "Long An": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=loan",
    "Sóc Trăng": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=sotr",
    "Tây Ninh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tani",
    "Tiền Giang": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tigi",
    "TP. Hồ Chí Minh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=tphc",
    "Trà Vinh": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=trvi",
    "Vĩnh Long": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=vilo",
    "Vũng Tàu": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=vuta",
    "Đà Nẵng": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dana",
    "Bình Định": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=bidi",
    "Đắk Lắk": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dalak",
    "Đắk Nông": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=dano",
    "Gia Lai": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=gila",
    "Khánh Hòa": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=khho",
    "Kon Tum": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=kotu",
    "Ninh Thuận": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=nith",
    "Phú Yên": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=phye",
    "Quảng Bình": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qubi",
    "Quảng Nam": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=quna",
    "Quảng Ngãi": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qung",
    "Quảng Trị": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=qutr",
    "Thừa Thiên Huế": "https://www.kqxs88.live/api/front/open/lottery/history/list/game?limitNum=60&gameCode=thth"
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

def _extract_loto(prize_content: str) -> List[str]:
    """Helper: Tách các số 2 số cuối từ chuỗi giải (có thể chứa nhiều số cách nhau bởi dấu phẩy)"""
    if not prize_content:
        return []
    # Tách theo dấu phẩy hoặc các ký tự ngăn cách thông thường
    raw_nums = re.split(r'[,\s-]+', prize_content)
    lotos = []
    for n in raw_nums:
        if n and n.isdigit():
            lotos.append(n[-2:])
    return lotos

def fetch_station_data(station_name: str, total_days: int = 60) -> List[Dict]:
    url_template = DAI_API.get(station_name)
    if not url_template:
        return []
    
    url = url_template.replace("limitNum=60", f"limitNum={total_days}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        data = response.json()
        
        if not data.get("success"):
            return []
        
        issue_list = data.get("t", {}).get("issueList", [])
        results = []
        
        for issue in issue_list[:total_days]:
            turn_num = issue.get("turnNum", "")
            detail = issue.get("detail", "")
            
            if not turn_num or not detail:
                continue
            
            try:
                prizes = json.loads(detail)
                # prizes: 0=DB, 1=G1 ... 6=G6, 7=G7, 8=G8
                
                result = {
                    "date": turn_num,
                    "db": prizes[0] if len(prizes) > 0 else "",
                    "g1": prizes[1] if len(prizes) > 1 else "",
                    "g6": prizes[6] if len(prizes) > 6 else "",
                    "g7": prizes[7] if len(prizes) > 7 else "",
                    "g8": prizes[8] if len(prizes) > 8 else "",
                }
                
                # Tách lô tô (2 số cuối) cho các giải cần thiết
                result["db_2so"] = _extract_loto(result["db"]) # Trả về list
                result["g1_2so"] = _extract_loto(result["g1"])
                result["g6_2so"] = _extract_loto(result["g6"])
                result["g7_2so"] = _extract_loto(result["g7"])
                result["g8_2so"] = _extract_loto(result["g8"])
                
                # Làm phẳng db và g1 vì thường chỉ có 1 số, nhưng giữ format list cho đồng nhất
                result["db_2so"] = result["db_2so"][0] if result["db_2so"] else ""
                result["g1_2so"] = result["g1_2so"][0] if result["g1_2so"] else ""

                results.append(result)
                
            except Exception as e:
                logging.error(f"Error parsing detail for {station_name}: {e}")
                continue
        
        return results
        
    except Exception as e:
        logging.error(f"Error fetching data for {station_name}: {e}")
        return []

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
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-dien-toan-123/{total_days}")
    data = []
    if not soup: return data
    try:
        divs = soup.find_all("div", class_="result_div", id="result_123")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date = _normalize_date(ds.text.strip()) if ds else ""
            if not date: continue
            tbl = div.find("table", id="result_tab_123")
            if tbl:
                row = tbl.find("tbody").find("tr")
                cells = row.find_all("td") if row else []
                if len(cells) == 3:
                    nums = [c.text.strip() for c in cells if c.text.strip().isdigit()]
                    if len(nums) == 3:
                        data.append({"date": date, "dt_numbers": nums})
    except Exception: pass
    return data

def fetch_than_tai(total_days: int) -> List[Dict]:
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-than-tai/{total_days}")
    data = []
    if not soup: return data
    try:
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date = _normalize_date(ds.text.strip()) if ds else ""
            if not date: continue
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
    except Exception: pass
    return data

def _parse_congcuxoso_loto(url: str, total_days: int) -> List[List[str]]:
    """Parse trả về list các số lô tô (2 số cuối) cho mỗi ngày"""
    soup = fetch_url(url)
    all_days_nums = []
    if not soup: return all_days_nums
    
    try:
        tbl = soup.find("table", id="MainContent_dgv")
        if tbl:
            rows = tbl.find_all("tr")[1:]
            for row in reversed(rows):
                cells = row.find_all("td")
                day_nums = []
                for cell in reversed(cells):
                    t = cell.text.strip()
                    if t and t not in ("-----", "\xa0"):
                        # Xử lý chuỗi có thể chứa nhiều số hoặc ký tự lạ
                        cleaned = ''.join(filter(str.isdigit, t))
                        if len(cleaned) >= 2:
                            # Lấy 2 số cuối
                            day_nums.append(cleaned[-2:])
                all_days_nums.append(day_nums)
    except Exception: pass
    return all_days_nums[:total_days]

def _parse_congcuxoso_single(url: str, total_days: int) -> List[str]:
    """Parse trả về 1 số duy nhất (ví dụ ĐB)"""
    raw_list = _parse_congcuxoso_loto(url, total_days)
    # Lấy số đầu tiên trong list mỗi ngày (vì ĐB/G1 ở đây thường chỉ có 1 ô)
    return [d[0] if d else "" for d in raw_list]

def fetch_xsmb_group(total_days: int) -> Tuple[List[str], List[str], List[List[str]]]:
    """
    Fetch XSMB: ĐB, G1, và G7
    Returns: (List ĐB, List G1, List [G7 numbers])
    """
    with concurrent.futures.ThreadPoolExecutor() as executor:
        f_db = executor.submit(_parse_congcuxoso_single, 
                            "https://congcuxoso.com/MienBac/DacBiet/PhoiCauDacBiet/PhoiCauTuan5So.aspx", total_days)
        f_g1 = executor.submit(_parse_congcuxoso_single, 
                            "https://congcuxoso.com/MienBac/GiaiNhat/PhoiCauGiaiNhat/PhoiCauTuan5So.aspx", total_days)
        f_g7 = executor.submit(_parse_congcuxoso_loto, 
                            "https://congcuxoso.com/MienBac/GiaiBay/PhoiCauGiaiBay/PhoiCauTuan5So.aspx", total_days)
        
        return f_db.result(), f_g1.result(), f_g7.result()

