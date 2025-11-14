from datetime import datetime

def convert_time(ms):
    if ms:
        return datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    return None