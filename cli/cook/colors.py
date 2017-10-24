import os
from blessed import Terminal

term = Terminal()
failed = term.bold_red
success = term.green
running = term.cyan
waiting = term.yellow
reason = term.red
bold = term.bold
wrap = term.wrap


def __ls_color(s, code, fallback_fn):
    """
    Parses the LS_COLORS environment variable to get consistent colors with the
    user's current setup, falling back to default formatting if the parsing fails
    """
    if term.is_a_tty and 'LS_COLORS' in os.environ:
        split_pairs = [p.split('=') for p in os.environ['LS_COLORS'].split(':')]
        matched_pairs = [p for p in split_pairs if len(p) == 2 and p[0] == code]
        if len(matched_pairs) > 0:
            return f'\033[{matched_pairs[0][1]}m{s}\033[0;0m'

    return fallback_fn(s)


def directory(s):
    """Attempts to use the "di" entry in LS_COLORS, falling back to cyan"""
    return __ls_color(s, 'di', term.cyan)


def executable(s):
    """Attempts to use the "ex" entry in LS_COLORS, falling back to green"""
    return __ls_color(s, 'ex', term.green)
