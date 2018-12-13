import os
import sys

import textwrap


MOVE_UP = '\033[F'


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


def failed(s):
    return colorize(s, Color.BOLD + Color.RED)


def success(s):
    return colorize(s, Color.GREEN)


def running(s):
    return colorize(s, Color.CYAN)


def waiting(s):
    return colorize(s, Color.YELLOW)


def reason(s):
    return colorize(s, Color.RED)


def bold(s):
    return colorize(s, Color.BOLD)


wrap = textwrap.wrap


def colorize(s, color):
    """Formats the given string with the given color"""
    return color + s + Color.END if tty() else s


def __ls_color(s, code, fallback_fn):
    """
    Parses the LS_COLORS environment variable to get consistent colors with the
    user's current setup, falling back to default formatting if the parsing fails
    """
    if tty() and 'LS_COLORS' in os.environ:
        split_pairs = [p.split('=') for p in os.environ['LS_COLORS'].split(':')]
        matched_pairs = [p for p in split_pairs if len(p) == 2 and p[0] == code]
        if len(matched_pairs) > 0:
            return f'\033[{matched_pairs[0][1]}m{s}\033[0;0m'

    return fallback_fn(s)


def tty():
    """Returns true if running in a real terminal (as opposed to being piped or redirected)"""
    return sys.stdout.isatty()


def directory(s):
    """Attempts to use the "di" entry in LS_COLORS, falling back to cyan"""
    return __ls_color(s, 'di', lambda t: colorize(t, Color.CYAN))


def executable(s):
    """Attempts to use the "ex" entry in LS_COLORS, falling back to green"""
    return __ls_color(s, 'ex', lambda t: colorize(t, Color.GREEN))
