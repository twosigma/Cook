#!/usr/bin/env python3
#
#  Copyright (c) 2020 Two Sigma Open Source, LLC
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  IN THE SOFTWARE.
#
"""Entrypoint for Cook's sidecar progress reporter."""

import faulthandler
import logging
import os
import requests
import signal
import sys

import cook.sidecar.config as csc
import cook.sidecar.tracker as cst
from cook.sidecar import util
from cook.sidecar.version import VERSION


def start_progress_trackers():
    try:
        logging.info(f'Starting cook.sidecar {VERSION} progress reporter')
        config = csc.initialize_config(os.environ)

        default_url = config.callback_url
        current_url = default_url

        def send_progress_message(message):
            nonlocal current_url
            try:
                for i in range(config.max_post_attempts):
                    response = requests.post(current_url, allow_redirects=False, timeout=config.max_post_time_secs, json=message)
                    if 200 <= response.status_code <= 299:
                        return True
                    elif response.is_redirect and response.status_code == 307:
                        current_url = response.headers['location']
                        logging.info(f'Redirected! Changed progress update callback url to: {current_url}')
                    else:
                        logging.warning(f'Unexpected progress update response ({response.status_code}): {response.content}')
                        break
                else:
                    logging.warning(f'Reached max redirect retries ({config.max_post_redirect_follow})')
            except Exception:
                logging.exception(f'Error raised while posting progress update to {current_url}')
            current_url = default_url
            logging.info(f'Failed to post progress update. Reset progress update callback url: {current_url}')
            return False

        max_message_length = config.max_message_length
        sample_interval_ms = config.progress_sample_interval_ms
        sequence_counter = cst.ProgressSequenceCounter()
        progress_updater = cst.ProgressUpdater(max_message_length, sample_interval_ms, send_progress_message)

        def launch_progress_tracker(progress_location, location_tag):
            progress_file_path = os.path.abspath(progress_location)
            logging.info(f'Location {progress_location} (absolute path={progress_file_path}) tagged as [tag={location_tag}]')
            progress_tracker = cst.ProgressTracker(config, sequence_counter, progress_updater, progress_location, location_tag)
            progress_tracker.start()
            return progress_tracker

        progress_locations = {config.progress_output_name: 'progress',
                              config.stderr_file(): 'stderr',
                              config.stdout_file(): 'stdout'}
        logging.info(f'Progress will be tracked from {len(progress_locations)} locations')
        progress_trackers = [launch_progress_tracker(file, name) for file, name in progress_locations.items()]

        def set_terminate_handler(handler):
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)

        def exit_on_interrupt(interrupt_code, _):
            sys.exit(f'Progress Reporter killed with code {interrupt_code}')

        def handle_interrupt(interrupt_code, _):
            logging.info(f'Progress Reporter interrupted with code {interrupt_code}')
            # allow a second signal to kill the process immediately (no blocking)
            set_terminate_handler(exit_on_interrupt)
            # force send the latest progress state if available, and stop the tracker
            for progress_tracker in progress_trackers:
                progress_tracker.stop()
            for progress_tracker in progress_trackers:
                progress_tracker.wait()

        def dump_traceback(signal, frame):
            faulthandler.dump_traceback()

        signal.signal(signal.SIGUSR1, dump_traceback)
        set_terminate_handler(handle_interrupt)

        return progress_trackers

    except Exception:
        logging.exception('Error starting Progress Reporter')
        return None


def await_progress_trackers(progress_trackers):
        if progress_trackers is None:
            sys.exit('Failed to start progress trackers')
        # wait for all background threads to exit
        # (but this process will probably be killed first instead)
        for progress_tracker in progress_trackers:
            progress_tracker.wait()


def main():
    util.init_logging()
    if len(sys.argv) == 2 and sys.argv[1] == "--version":
        print(VERSION)
    else:
        progress_trackers = start_progress_trackers()
        await_progress_trackers(progress_trackers)


if __name__ == '__main__':
    main()
