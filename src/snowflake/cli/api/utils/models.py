import os


class DictWithEnvironFallback(dict):
    def __getattr__(self, item):
        return self[item]

    def __getitem__(self, item):
        if item in os.environ:
            return os.environ[item]
        return super().__getitem__(item)

    def __contains__(self, item):
        return item in os.environ or super().__contains__(item)
