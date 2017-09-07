class Codes:
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    REASON = '\033[38;5;3m'
    UNDERLINE = '\033[4m'
    SUCCESS = '\033[38;5;22m'
    RUNNING = '\033[38;5;36m'
    FAILED = '\033[91m'
    WAITING = '\033[38;5;130m'


def failed(s):
    return Codes.FAILED + Codes.BOLD + s + Codes.ENDC


def success(s):
    return Codes.SUCCESS + Codes.BOLD + s + Codes.ENDC


def running(s):
    return Codes.RUNNING + Codes.BOLD + s + Codes.ENDC


def waiting(s):
    return Codes.WAITING + Codes.BOLD + s + Codes.ENDC


def reason(s):
    return Codes.REASON + s + Codes.ENDC


def bold(s):
    return Codes.BOLD + s + Codes.ENDC
