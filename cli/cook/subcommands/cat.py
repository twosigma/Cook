from functools import partial

from cook.mesos import read_file
from cook.querying import parse_entity_refs, query_unique_and_run
from cook.util import guard_no_cluster


def cat_for_instance(instance, sandbox_dir, path):
    """Outputs the contents of the Mesos sandbox path for the given instance."""
    read = partial(read_file, instance=instance, sandbox_dir=sandbox_dir, path=path)
    offset = 0
    chunk_size = 4096
    data = read(offset=offset, length=chunk_size)['data']
    while len(data) > 0:
        print(data, end='')
        offset = offset + len(data)
        data = read(offset=offset, length=chunk_size)['data']


def cat(clusters, args, _):
    """Outputs the contents of the corresponding Mesos sandbox path by job or instance uuid."""
    guard_no_cluster(clusters)
    entity_refs, clusters_of_interest = parse_entity_refs(clusters, args.get('uuid'))
    paths = args.get('path')

    if len(entity_refs) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single uuid.')

    if len(paths) > 1:
        # argparse should prevent this, but we'll be defensive anyway
        raise Exception(f'You can only provide a single path.')

    command_fn = partial(cat_for_instance, path=paths[0])
    query_unique_and_run(clusters_of_interest, entity_refs[0], command_fn)


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    parser = add_parser('cat', help='output files by job or instance uuid')
    parser.add_argument('uuid', nargs=1)
    parser.add_argument('path', nargs=1)
    return cat
