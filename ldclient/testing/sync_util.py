import time


def wait_until(condition, timeout=5):
    end_time = time.time() + timeout

    while True:
        result = condition()
        if result:
            return result
        elif time.time() > end_time:
            raise Exception("Timeout waiting for {0}".format(
                condition.__name__))  # pragma: no cover
        else:
            time.sleep(.1)
