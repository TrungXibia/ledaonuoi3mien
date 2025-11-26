import requests
import concurrent.futures
from bs4 import BeautifulSoup
import logging
import time
from typing import List, Dict, Tuple
import json

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
    """
    Get list of lottery stations for a specific region and day.
    
    Args:
        region: "Miền Nam" or "Miền Trung"
        day: Day of week (e.g., "Thứ 2", "Chủ Nhật")
    
    Returns:
        List of station names
    """
    if region == "Miền Nam":
        return LICH_QUAY_NAM.get(day, [])
    elif region == "Miền Trung":
        return LICH_QUAY_TRUNG.get(day, [])
    return []

def get_all_stations_in_region(region: str) -> List[str]:
    """
    Get all unique stations for a region across all days.
    """
    stations = set()
    schedule = {}
    if region == "Miền Nam":
        schedule = LICH_QUAY_NAM
    elif region == "Miền Trung":
        schedule = LICH_QUAY_TRUNG
        
    for day_stations in schedule.values():
        stations.update(day_stations)
        
    return sorted(list(stations))

def fetch_station_data(station_name: str, total_days: int = 60) -> List[Dict]:
    """
    Fetch lottery data for a specific station from API.
    
    Args:
        station_name: Name of the station (e.g., "An Giang")
        total_days: Number of days to fetch
    
    Returns:
        List of lottery results with date and prizes
    """
    url_template = DAI_API.get(station_name)
    if not url_template:
        logging.error(f"No API URL found for station: {station_name}")
        return []
    
    # Replace limitNum=60 with limitNum={total_days}
    url = url_template.replace("limitNum=60", f"limitNum={total_days}")
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if not data.get("success"):
            logging.error(f"API returned error for {station_name}")
            return []
        
        issue_list = data.get("t", {}).get("issueList", [])
        results = []
        
        for issue in issue_list[:total_days]:
            turn_num = issue.get("turnNum", "")  # Format: "20/11/2025"
            detail = issue.get("detail", "")
            
            if not turn_num or not detail:
                continue
            
            try:
                # Parse detail JSON array
                prizes = json.loads(detail)
                
                # Extract prizes based on structure
                # prizes[0] = ĐB, prizes[1] = G1, etc.
                result = {
                    "date": turn_num,
                    "db": prizes[0] if len(prizes) > 0 else "",  # Đặc Biệt
                    "g1": prizes[1] if len(prizes) > 1 else "",  # Giải Nhất
                    "g2": prizes[2] if len(prizes) > 2 else "",  # Giải Nhì
                    "g3": prizes[3] if len(prizes) > 3 else "",  # Giải Ba (2 số)
                    "g4": prizes[4] if len(prizes) > 4 else "",  # Giải Tư (7 số)
                    "g5": prizes[5] if len(prizes) > 5 else "",  # Giải Năm
                    "g6": prizes[6] if len(prizes) > 6 else "",  # Giải Sáu (3 số)
                    "g7": prizes[7] if len(prizes) > 7 else "",  # Giải Bảy
                    "g8": prizes[8] if len(prizes) > 8 else "",  # Giải Tám
                }
                
                # Extract 2-digit numbers (lô) from ĐB and G1
                result["db_2so"] = result["db"][-2:] if result["db"] else ""
                result["g1_2so"] = result["g1"][-2:] if result["g1"] else ""
                
                # Extract 2-digit numbers from G6, G7, G8 (for Nam/Trung comparison)
                # G6 có 3 số, G7 có 4 số, G8 có 1 số (theo format xổ số miền Nam/Trung)
                g6_numbers = []
                if result["g6"]:
                    g6_list = result["g6"].split(",") if "," in result["g6"] else [result["g6"]]
                    g6_numbers = [num.strip()[-2:] for num in g6_list if num.strip()]
                
                g7_numbers = []
                if result["g7"]:
                    g7_list = result["g7"].split(",") if "," in result["g7"] else [result["g7"]]
                    g7_numbers = [num.strip()[-2:] for num in g7_list if num.strip()]
                
                g8_numbers = []
                if result["g8"]:
                    g8_list = result["g8"].split(",") if "," in result["g8"] else [result["g8"]]
                    g8_numbers = [num.strip()[-2:] for num in g8_list if num.strip()]
                
                # Gộp tất cả số từ G6, G7, G8
                result["g678_2so"] = g6_numbers + g7_numbers + g8_numbers
                
                results.append(result)

                
            except json.JSONDecodeError as e:
                logging.error(f"Error parsing detail for {station_name}: {e}")
                continue
        
        return results
        
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data for {station_name}: {e}")
        return []
    except Exception as e:
        logging.error(f"Unexpected error for {station_name}: {e}")
        return []

def fetch_url(url: str, max_retries: int = 3) -> BeautifulSoup:
    """
    Fetch URL with retry logic and better error handling.
    
    Args:
        url: URL to fetch
        max_retries: Maximum number of retry attempts
        
    Returns:
        BeautifulSoup object or None if failed
    """
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=10)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except requests.exceptions.Timeout:
            logging.warning(f"Timeout loading {url}, attempt {attempt + 1}/{max_retries}")
            if attempt < max_retries - 1:
                time.sleep(1)  # Wait before retry
        except requests.exceptions.RequestException as e:
            logging.error(f"Error loading {url}: {e}")
            if attempt < max_retries - 1:
                time.sleep(1)
            else:
                return None
    return None

def _normalize_date(date_str: str) -> str:
    """
    Normalize date string from 'Thứ Hai ngày 24-11-2025' to '24/11/2025'.
    Also handles '24-11-2025' -> '24/11/2025'.
    """
    try:
        # Remove "Thứ ... ngày " prefix if present
        if "ngày" in date_str:
            date_str = date_str.split("ngày")[-1].strip()
        
        # Replace - with /
        return date_str.replace("-", "/")
    except:
        return date_str

def fetch_dien_toan(total_days: int) -> List[Dict]:
    """Fetch Điện Toán 123 data with validation."""
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-dien-toan-123/{total_days}")
    data = []
    
    if not soup:
        logging.error("Failed to fetch Điện Toán data")
        return data
        
    try:
        divs = soup.find_all("div", class_="result_div", id="result_123")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            
            if not date_raw:
                continue
            
            date = _normalize_date(date_raw)
                
            tbl = div.find("table", id="result_tab_123")
            if tbl:
                row = tbl.find("tbody").find("tr")
                cells = row.find_all("td") if row else []
                if len(cells) == 3:
                    nums = [c.text.strip() for c in cells]
                    # Validate numbers
                    if all(n.isdigit() for n in nums):
                        data.append({"date": date, "dt_numbers": nums})
    except Exception as e:
        logging.error(f"Error parsing Điện Toán data: {e}")
    
    return data

def fetch_than_tai(total_days: int) -> List[Dict]:
    """Fetch Thần Tài data with validation."""
    soup = fetch_url(f"https://ketqua04.net/so-ket-qua-than-tai/{total_days}")
    data = []
    
    if not soup:
        logging.error("Failed to fetch Thần Tài data")
        return data
        
    try:
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            
            if not date_raw:
                continue
            
            date = _normalize_date(date_raw)
                
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                # Validate: should be 4 digits
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
        divs = soup.find_all("div", class_="result_div", id="result_tt4")
        for div in divs[:total_days]:
            ds = div.find("span", id="result_date")
            date_raw = ds.text.strip() if ds else ""
            
            if not date_raw:
                continue
            
            date = _normalize_date(date_raw)
                
            tbl = div.find("table", id="result_tab_tt4")
            if tbl:
                cell = tbl.find("td", id="rs_0_0")
                num = cell.text.strip() if cell else ""
                # Validate: should be 4 digits
                if num.isdigit() and len(num) == 4:
                    data.append({"date": date, "tt_number": num})
    except Exception as e:
        logging.error(f"Error parsing Thần Tài data: {e}")
    
    return data

def fetch_xsmb_full(total_days: int) -> List[Dict]:
    """
    Fetch complete XSMB data including all prizes (ĐB, G1, G2, G3, G4, G5, G6, G7).
    Returns a list of dictionaries with date and all prize information.
    """
    url = f"https://ketqua04.net/so-ket-qua-xo-so-mien-bac/{total_days}"
    soup = fetch_url(url)
    data = []
    
    if not soup:
        logging.error("Failed to fetch XSMB data")
        return data
    
    try:
        # Find result tables
        tables = soup.find_all("table", {"class": "table-result"})
        
        for table in tables[:total_days]:
            # Get date from the table header or nearby element
            date_elem = table.find_previous("span", {"class": "date"}) or table.find_previous("h3")
            if not date_elem:
                continue
            
            date_raw = date_elem.text.strip()
            date = _normalize_date(date_raw)
            
            # Extract prizes
            result = {"date": date}
            
            rows = table.find_all("tr")
            for row in rows:
                prize_label_elem = row.find("td", {"class": "prize-label"})
                if not prize_label_elem:
                    continue
                
                prize_label = prize_label_elem.text.strip().upper()
                prize_values_elem = row.find("td", {"class": "prize-value"})
                if not prize_values_elem:
                    continue
                
                prize_text = prize_values_elem.text.strip()
                
                # Map prize labels to dictionary keys
                if "ĐẶC BIỆT" in prize_label or "ĐB" in prize_label:
                    result["db"] = prize_text
                    result["db_2so"] = prize_text[-2:] if prize_text else ""
                elif "GIẢI NHẤT" in prize_label or "G1" in prize_label:
                    result["g1"] = prize_text
                    result["g1_2so"] = prize_text[-2:] if prize_text else ""
                elif "GIẢI SÁU" in prize_label or "G6" in prize_label or "GIẢI 6" in prize_label:
                    # G6 có 3 số, tách bằng dấu cách
                    g6_numbers = [n.strip() for n in prize_text.split() if n.strip()]
                    result["g6"] = ",".join(g6_numbers)
                    result["g6_2so"] = [n[-2:] for n in g6_numbers if n]
                elif "GIẢI BẢY" in prize_label or "G7" in prize_label or "GIẢI 7" in prize_label:
                    # G7 có 4 số, tách bằng dấu cách
                    g7_numbers = [n.strip() for n in prize_text.split() if n.strip()]
                    result["g7"] = ",".join(g7_numbers)
                    result["g7_2so"] = [n[-2:] for n in g7_numbers if n]
            
            # Combine G6 and G7 2-digit numbers for easy comparison
            g6_list = result.get("g6_2so", [])
            g7_list = result.get("g7_2so", [])
            result["g67_2so"] = g6_list + g7_list
            
            if result.get("db") and result.get("g1"):  # At least have basic data
                data.append(result)
    
    except Exception as e:
        logging.error(f"Error parsing XSMB data: {e}")
    
    return data

def fetch_xsmb_group(total_days: int) -> Tuple[List[str], List[str]]:
    """
    Legacy function for backward compatibility.
    Returns (ĐB numbers, G1 numbers) as before.
    """
    full_data = fetch_xsmb_full(total_days)
    db_list = [item.get("db", "") for item in full_data]
    g1_list = [item.get("g1", "") for item in full_data]
    return db_list, g1_list
