import time


def current_time_millis() -> int:
    return int(time.time() * 1000)
