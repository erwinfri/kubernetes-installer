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

def get_var(name, spec, default=None):
    return EXTRA_VARS.get(name) or spec.get(name) or default
