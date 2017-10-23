import os
from blessed import Terminal

term = Terminal()


def failed(s):
    return term.bold_red(s)


def success(s):
    return term.green(s)


def running(s):
    return term.cyan(s)


def waiting(s):
    return term.yellow(s)


def reason(s):
    return term.red(s)


def bold(s):
    return term.bold(s)


def __ls_color(s, code, fallback_fn):
    """
    Parses the LS_COLORS environment variable to get consistent colors with the
    user's current setup, falling back to default formatting if the parsing fails
    """
    if 'LS_COLORS' in os.environ:
        split_pairs = [p.split('=') for p in os.environ['LS_COLORS'].split(':')]
        matched_pairs = [p for p in split_pairs if len(p) == 2 and p[0] == code]
        if len(matched_pairs) > 0:
            return f'\033[{matched_pairs[0][1]}m{s}\033[0;0m'

    return fallback_fn(s)


def directory(s):
    return __ls_color(s, 'di', term.cyan)


def executable(s):
    return __ls_color(s, 'ex', term.green)
