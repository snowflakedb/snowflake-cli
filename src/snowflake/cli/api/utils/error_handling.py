from contextlib import contextmanager


@contextmanager
def ignore_exceptions():
    try:
        yield
    except:
        pass
