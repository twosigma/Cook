import argparse
import logging
from functools import partial

from cook.mesos import read_file
from cook.querying import parse_entity_refs, query_unique_and_run, parse_entity_ref
from cook.util import guard_no_cluster


def cat_for_instance(instance, sandbox_dir, path):
    """Outputs the contents of the Mesos sandbox path for the given instance."""
    read = partial(read_file, instance=instance, sandbox_dir=sandbox_dir, path=path)
    offset = 0
    chunk_size = 4096
    data = read(offset=offset, length=chunk_size)['data']
    try:
        while len(data) > 0:
            print(data, end='')
            offset = offset + len(data)
            data = read(offset=offset, length=chunk_size)['data']
    except BrokenPipeError as bpe:
        logging.exception(bpe)


def cat(clusters, args, _):
    """Outputs the contents of the corresponding Mesos sandbox path by job or instance uuid."""
    guard_no_cluster(clusters)
    entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('target-entity'))
    paths = args.get('path')

    # argparse should prevent these, but we'll be defensive anyway
    assert len(entity_refs) == 1, 'Only a single UUID or URL is supported.'
    assert len(paths) == 1, 'Only a single path is supported.'

    command_fn = partial(cat_for_instance, path=paths[0])
    query_unique_and_run(clusters_of_interest, entity_refs[0], command_fn)


def valid_entity_ref(s):
    """Allows argparse to flag user-provided entity ref strings as valid or not"""
    try:
        parse_entity_ref(s, lambda x: x)
        return s
    except Exception as e:
        raise argparse.ArgumentTypeError(str(e))


def valid_path(s):
    """Allows argparse to flag user-provided paths as valid or not"""
    if len(s) > 0:
        return s
    else:
        raise argparse.ArgumentTypeError('path cannot be empty')


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('cat', help='output files by job or instance uuid')
    parser.add_argument('target-entity', nargs=1,
                        help='Accepts either a job or an instance UUID or URL. The latest instance is selected for a '
                             'job with multiple instances.',
                        type=valid_entity_ref)
    parser.add_argument('path', nargs=1,
                        help='Relative to the sandbox directory on the Mesos agent where the instance runs.',
                        type=valid_path)
    return cat
