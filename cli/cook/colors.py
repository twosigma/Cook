import os
import sys

import textwrap


class Color:
    PURPLE = '\033[95m'
    CYAN = '\033[96m'
    DARKCYAN = '\033[36m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


failed = lambda s: Color.BOLD + Color.RED + s + Color.END
success = lambda s: Color.GREEN + s + Color.END
running = lambda s: Color.CYAN + s + Color.END
waiting = lambda s: Color.YELLOW + s + Color.END
reason = lambda s: Color.RED + s + Color.END
bold = lambda s: Color.BOLD + s + Color.END
wrap = textwrap.fill


def __ls_color(s, code, fallback_fn):
    """
    Parses the LS_COLORS environment variable to get consistent colors with the
    user's current setup, falling back to default formatting if the parsing fails
    """
    if sys.stdout.isatty() and 'LS_COLORS' in os.environ:
        split_pairs = [p.split('=') for p in os.environ['LS_COLORS'].split(':')]
        matched_pairs = [p for p in split_pairs if len(p) == 2 and p[0] == code]
        if len(matched_pairs) > 0:
            return f'\033[{matched_pairs[0][1]}m{s}\033[0;0m'

    return fallback_fn(s)


def directory(s):
    """Attempts to use the "di" entry in LS_COLORS, falling back to cyan"""
    return __ls_color(s, 'di', lambda t: Color.CYAN + t + Color.END)


def executable(s):
    """Attempts to use the "ex" entry in LS_COLORS, falling back to green"""
    return __ls_color(s, 'ex', lambda t: Color.GREEN + t + Color.END)
