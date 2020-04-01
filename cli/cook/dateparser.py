import datetime
import logging

PATTERN_TO_TIMEDELTA_FN = (
    (r'^(\d+) sec(?:ond)?s? ago$', lambda x: datetime.timedelta(seconds=x)),
    (r'^(\d+) min(?:ute)?s? ago$', lambda x: datetime.timedelta(minutes=x)),
    (r'^(\d+) hours? ago$', lambda x: datetime.timedelta(hours=x)),
    (r'^(\d+) days? ago$', lambda x: datetime.timedelta(days=x)),
    (r'^(\d+) weeks? ago$', lambda x: datetime.timedelta(weeks=x))
)


def parse(date_time_string, time_zone):
    """
    Parses the given date_time_string and constructs a datetime object.
    Accepts strings in the following formats, where x is any integer:

    - now
    - today
    - yesterday
    - x seconds ago
    - x minutes ago
    - x hours ago
    - x days ago
    - x weeks ago
    - any format supported by dateutil's parser

    Why did we roll our own datetime parsing function?
    The existing libraries that do this sort of parsing also provide
    additional features such as multi-language support which:

    - add complexity we don't want
    - slow them down
    - make pyinstaller compatibility hard or impossible
    """
    date_time_string = date_time_string.strip()
    date_time_string_lower = date_time_string.lower()
    now = datetime.datetime.now(tz=time_zone)

    if date_time_string_lower in ('now', 'today'):
        return now

    if date_time_string_lower == 'yesterday':
        return now - datetime.timedelta(days=1)

    import re
    for pattern, timedelta_fn in PATTERN_TO_TIMEDELTA_FN:
        match = re.match(pattern, date_time_string_lower)
        if match:
            return now - timedelta_fn(int(match.groups()[0]))

    try:
        from dateutil import parser
        dt = parser.parse(date_time_string, ignoretz=False)
        if dt:
            if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
                dt = time_zone.localize(dt)
            return dt
    except ValueError as ve:
        logging.exception(ve)

    return None
