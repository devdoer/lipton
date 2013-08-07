_global_cfg = None

def install_cfg(cfg):
    global _global_cfg
    _global_cfg = cfg
def get(name):
    global _global_cfg
    return getattr(_global_cfg, name, None)
