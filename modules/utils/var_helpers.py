import argparse

def parse_extra_vars():
    parser = argparse.ArgumentParser()
    parser.add_argument('-e', '--extra', action='append', default=[], help='Extra KEY=VALUE pairs')
    args, unknown = parser.parse_known_args()
    extra_vars = {}
    for item in args.extra:
        if '=' in item:
            k, v = item.split('=', 1)
            extra_vars[k] = v
    return extra_vars

EXTRA_VARS = parse_extra_vars()

import os
def get_var(name, spec, default=None):
    env_val = os.environ.get(name)
    if env_val is None or env_val == "":
        env_val = os.environ.get(name.upper())
    if env_val is not None and env_val != "":
        return env_val
    extra_val = EXTRA_VARS.get(name)
    if extra_val is not None and extra_val != "":
        return extra_val
    spec_val = spec.get(name)
    if spec_val is not None and spec_val != "":
        return spec_val
    return default
