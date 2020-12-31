import argparse
import logging
import os
import sys
from functools import partial

from cook import plugins
from cook.mesos import download_file
from cook.querying import parse_entity_refs, query_unique_and_run, parse_entity_ref
from cook.util import guard_no_cluster

def cat_using_download_file(instance, sandbox_dir_fn, path):
    retrieve_fn = plugins.get_fn('download-job-instance-file', download_file)
    download = retrieve_fn(instance, sandbox_dir_fn, path)
    try:
        for data in download(chunk_size=4096):
            if data:
                sys.stdout.buffer.write(data)
    except BrokenPipeError as bpe:
        sys.stderr.close()
        logging.exception(bpe)

def kubectl_cat_instance_file(instance_uuid, path):
    os.execlp('kubectl', 'kubectl',
              'exec',
              '-c', os.getenv('COOK_CONTAINER_NAME_FOR_JOB', 'required-cook-job-container'),
              '-it', instance_uuid,
              '--', 'cat', path)

def cat_for_instance(_, instance, sandbox_dir_fn, __, path):
    """
    Outputs the contents of the Mesos sandbox path for the given instance.
    When using Kubernetes, calls the exec command of the kubectl cli.
    """
    compute_cluster = instance["compute-cluster"]
    compute_cluster_type = compute_cluster["type"]
    if compute_cluster_type == "kubernetes" and ("end_time" not in instance or instance["end_time"] is None):
        kubernetes_cat_instance_file_fn = plugins.get_fn('kubernetes-cat-for-instance', kubectl_cat_instance_file)
        kubernetes_cat_instance_file_fn(instance["task_id"], path)
    else:
        cat_using_download_file(instance, sandbox_dir_fn, path)


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
