
ENCODING = "utf-8"

def get_str(a_str_or_bytes):
    if isinstance(a_str_or_bytes,bytes):
        return a_str_or_bytes.decode(ENCODING)
    return a_str_or_bytes
    