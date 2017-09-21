from blessings import Terminal

t = Terminal()


def failed(s):
    return t.bold_red(s)


def success(s):
    return t.blue(s)


def running(s):
    return t.cyan(s)


def waiting(s):
    return t.yellow(s)


def reason(s):
    return t.red(s)


def bold(s):
    return t.bold(s)
