from contextlib import contextmanager


@contextmanager
def safe():
    try:
        yield
    except:
        pass
