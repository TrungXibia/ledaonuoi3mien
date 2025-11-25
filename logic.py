--- START OF FILE logic.py ---

from itertools import combinations

# --- GIỮ NGUYÊN CÁC DICT (BO_DICT, KEP_DICT...) ---
# (Để tiết kiệm không gian, tôi không paste lại phần Dictionary vì nó không đổi)
# Chỉ thêm hàm mới ở dưới cùng

def tao_dan_lien_tiep(source_str: str) -> list:
    """
    Tạo dàn lô từ chuỗi số bằng cách ghép 2 số liên tiếp và đảo lại.
    Ví dụ: 1234 -> 12, 21, 23, 32, 34, 43
    """
    if not source_str or len(source_str) < 2:
        return []
    
    dan = set()
    # Duyệt qua chuỗi, lấy từng cặp số liền kề
    for i in range(len(source_str) - 1):
        pair = source_str[i:i+2]
        if pair.isdigit():
            dan.add(pair)
            dan.add(pair[::-1]) # Đảo ngược (vd: 12 -> 21)
            
    return sorted(list(dan))

# ... Giữ lại các hàm cũ (bo, kep, hieu, zodiac...) nếu cần thiết cho phần khác
# Hoặc copy lại toàn bộ file logic.py cũ và thêm hàm trên.
