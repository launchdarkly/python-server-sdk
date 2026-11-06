import time


def rewrite_in_place(path: str, content: str, pause: float) -> None:
    """
    Rewrites a file the way applications and editors often do, without replacing it atomically:
    the file is truncated, and the new content is written after the pause. A read during the
    pause finds the file empty. A file that does not exist is created with the content.
    """
    try:
        f = open(path, 'r+')
    except FileNotFoundError:
        with open(path, 'w') as created:
            created.write(content)
        return
    with f:
        f.truncate(0)
        f.flush()
        time.sleep(pause)
        f.write(content)


def truncate(path: str) -> None:
    """Empties a file in place, which is how an in-place rewrite starts."""
    with open(path, 'r+') as f:
        f.truncate(0)
