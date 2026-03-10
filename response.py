def response(status: int, success: bool, message: str, records=None) -> dict:
    return {"status": status, "success": success, "message": message, "data": records}
