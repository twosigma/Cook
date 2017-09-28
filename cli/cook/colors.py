from blessings import Terminal

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
