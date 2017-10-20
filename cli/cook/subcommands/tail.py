import logging
import os
import time

from functools import partial

from cook import mesos, http

from cook.querying import query_unique_and_run

from cook.util import strip_all, check_positive

CHUNK_SIZE = 4096
LINE_DELIMITER = '\n'
DEFAULT_NUM_LINES = 10
DEFAULT_FOLLOW_SLEEP_SECS = 1.0


def read_file(instance, sandbox_dir, path, offset=None, length=None):
    """Calls the Mesos agent files/read API for the given path, offset, and length"""
    logging.info(f'reading file from sandbox {sandbox_dir} with path {path} at offset {offset} and length {length}')
    agent_url = mesos.instance_to_agent_url(instance)
    params = {'path': os.path.join(sandbox_dir, path or '')}
    if offset is not None:
        params['offset'] = offset
    if length is not None:
        params['length'] = length

    resp = http.__get(f'{agent_url}/files/read', params=params)
    if resp.status_code == 404:
        raise Exception(f"Cannot open '{path}' for reading (file was not found).")

    if resp.status_code != 200:
        logging.error(f'mesos agent returned status code {resp.status_code} and body {resp.text}')
        raise Exception('Encountered error when reading file from Mesos agent.')

    return resp.json()


def print_lines(lines):
    """Prints the given list of lines, delimited, and with no trailing newline"""
    print(LINE_DELIMITER.join(lines), end='')


def tail_for_instance(instance, sandbox_dir, path, num_lines_to_print, follow, follow_sleep_seconds):
    """
    Tails the contents of the Mesos sandbox path for the given instance. The
    algorithm reads chunks backwards from the end of the file and splits them into
    lines as it goes. If it finds that enough lines have been read to satisfy the
    user's request, or if it reaches the beginning of the file, it stops. Note that
    this assumes files will not shrink.
    """
    read = partial(read_file, instance=instance, sandbox_dir=sandbox_dir, path=path)

    # Get the current file size
    file_size = read()['offset']

    # Initialize loop variables
    offset = max(file_size - CHUNK_SIZE, 0)
    length = file_size - offset
    text_buffer = ''
    line_buffer = []

    while True:
        # Read the data at offset and length
        resp = read(offset=offset, length=length)
        data = resp['data']

        # Add to our buffer of text we've read from the agent
        text_buffer = data + text_buffer

        # Attempt to split into lines
        lines = text_buffer.split(LINE_DELIMITER)
        if len(lines) > 1:
            index_first_delimiter = len(lines[0])
            text_buffer = text_buffer[:index_first_delimiter]
            line_buffer = lines[1:] + line_buffer

        # Check if we've read enough lines, and if the last line
        # is empty, don't count it as a line that we care about
        last_line_empty = line_buffer[-1] == ''
        num_lines_printable = len(line_buffer) - (1 if last_line_empty else 0)
        if num_lines_printable >= num_lines_to_print:
            if last_line_empty:
                num_lines_to_print = num_lines_to_print + 1
            print_lines(line_buffer[-num_lines_to_print:])
            break

        # Check if we've reached the start of the file
        if offset == 0:
            print(text_buffer)
            print_lines(line_buffer)
            break

        # Update our offset and length
        new_offset = max(offset - CHUNK_SIZE, 0)
        length = offset - new_offset
        offset = new_offset

    if not follow:
        return

    # Follow as the file grows
    offset = file_size
    length = CHUNK_SIZE
    while True:
        resp = read(offset=offset, length=length)
        data = resp['data']
        num_chars_read = len(data)
        if num_chars_read > 0:
            print(data, end='')
            offset = offset + num_chars_read

        time.sleep(follow_sleep_seconds)


def tail(clusters, args):
    """Tails the contents of the corresponding Mesos sandbox path by job or instance uuid."""
    uuids = strip_all(args.get('uuid'))
    paths = strip_all(args.get('path'))
    lines = args.get('lines')
    follow = args.get('follow')
    sleep_interval = args.get('sleep-interval')

    if len(uuids) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    if len(paths) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single path.')

    command_fn = partial(tail_for_instance, path=paths[0], num_lines_to_print=lines,
                         follow=follow, follow_sleep_seconds=sleep_interval)
    query_unique_and_run(clusters, uuids[0], command_fn)


def register(add_parser, add_defaults):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('tail', help='output the last part of files in the Mesos sandbox by job or instance uuid')
    parser.add_argument('--lines', '-n', help=f'output the last NUM lines (default {DEFAULT_NUM_LINES})',
                        metavar='NUM', type=check_positive)
    parser.add_argument('--follow', '-f', help='output appended data as the file grows', action='store_true')
    parser.add_argument('--sleep-interval', '-s',
                        help=f'with -f, sleep for N seconds (default {DEFAULT_FOLLOW_SLEEP_SECS}) between iterations',
                        metavar='N')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('path', nargs=1)

    add_defaults('tail', {'lines': DEFAULT_NUM_LINES, 'sleep-interval': DEFAULT_FOLLOW_SLEEP_SECS})

    return tail
