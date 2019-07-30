try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

try:
    import click
except ImportError:
    class click:
        def __getattr__(self, item):
            def fn(obj, *args, **kwargs):
                return obj
            return fn

        def __bool__(self):
            return False
