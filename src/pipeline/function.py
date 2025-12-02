def transform(data: dict) -> dict:
    """
    Hàm biến đổi dữ liệu.
    Input: dict (từ queue)
    Output: dict (để load vào kho)
    """
    # Giả sử logic: Chuyển tên thành in hoa và tính năm sinh
    # Lỗi tiềm ẩn: nếu 'age' không phải số, code này sẽ crash -> kích hoạt Agent
    return {
        "processed_name": data["name"].upper(),
        "birth_year": 2024 - int(data["age"]),
        "original_data": data
    }