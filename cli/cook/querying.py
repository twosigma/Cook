import concurrent
import logging
import os
import sys
from collections import defaultdict
from concurrent import futures
from functools import partial
from operator import itemgetter
from urllib.parse import urlparse, parse_qs

from cook import http, terminal, mesos, progress
from cook.exceptions import CookRetriableException
from cook.util import is_valid_uuid, wait_until, print_info, distinct, partition


class Types:
    JOB = 'job'
    INSTANCE = 'instance'
    GROUP = 'group'
    ALL = '*'


class Clusters:
    ALL = '*'


def __query_cluster(cluster, uuids, pred, timeout, interval, make_request_fn, entity_type):
    """
    Queries the given cluster for the given uuids with
    an optional predicate, pred, that must be satisfied
    """

    def satisfy_pred():
        return pred(http.make_data_request(cluster, lambda: make_request_fn(cluster, uuids)))

    entities = http.make_data_request(cluster, lambda: make_request_fn(cluster, uuids))
    num_entities = len(entities)
    if pred and num_entities > 0:
        s = 's' if num_entities > 1 else ''
        num_string = terminal.bold(str(num_entities))
        if entity_type == Types.JOB:
            wait_text = f'Waiting for {num_string} job{s}'
        elif entity_type == Types.INSTANCE:
            wait_text = f'Waiting for instances of {num_string} job{s}'
        elif entity_type == Types.GROUP:
            wait_text = f'Waiting for {num_string} job group{s}'
        else:
            raise Exception(f'Invalid entity type {entity_type}.')

        wait_text = f'{wait_text} on {terminal.bold(cluster["name"])}'
        index = progress.add(wait_text)
        if pred(entities):
            progress.update(index, terminal.bold('Done'))
        else:
            entities = wait_until(satisfy_pred, timeout, interval)
            if entities:
                progress.update(index, terminal.bold('Done'))
            else:
                raise TimeoutError('Timeout waiting for response.')
    return entities


def query_cluster(cluster, uuids, pred, timeout, interval, make_request_fn, entity_type):
    """Delegates to __query_cluster in batches of at most 100 UUIDs and combines the results"""
    if len(uuids) == 0:
        return []

    # Cook will give us back two copies if the user asks for the same UUID twice, e.g.
    # $ cs show d38ea6bd-8a26-4ddf-8a93-5926fa2991ce d38ea6bd-8a26-4ddf-8a93-5926fa2991ce
    # Prevent this by calling distinct:
    uuids = distinct(uuids)

    entities = []
    query_batch_size = 100
    for uuid_batch in partition(uuids, query_batch_size):
        entity_batch = __query_cluster(cluster, uuid_batch, pred, timeout, interval, make_request_fn, entity_type)
        entities.extend(entity_batch)
    return entities


def make_job_request(cluster, uuids):
    """Attempts to query jobs corresponding to the given uuids from cluster."""
    return http.get(cluster, 'rawscheduler', params={'job': uuids, 'partial': 'true'})


def make_instance_request(cluster, uuids):
    """Attempts to query instances corresponding to the given uuids from cluster."""
    return http.get(cluster, 'rawscheduler', params={'instance': uuids, 'partial': 'true'})


def make_group_request(cluster, uuids):
    """Attempts to query groups corresponding to the given uuids from cluster."""
    return http.get(cluster, 'group', params={'uuid': uuids, 'partial': 'true', 'detailed': 'true'})


def entity_refs_to_uuids(cluster, entity_refs):
    """Given a cluster and list of entity ref maps, returns a map of type -> list of uuid"""
    uuids = defaultdict(list)
    for ref in entity_refs:
        if ref['cluster'].lower() in (cluster['name'].lower(), Clusters.ALL):
            ref_type = ref['type']
            ref_uuid = ref['uuid']
            if ref_type == Types.ALL:
                uuids[Types.JOB].append(ref_uuid)
                uuids[Types.INSTANCE].append(ref_uuid)
                uuids[Types.GROUP].append(ref_uuid)
            else:
                uuids[ref_type].append(ref_uuid)
    return uuids


def query_entities(cluster, entity_refs, pred_jobs, pred_instances, pred_groups, timeout, interval):
    """Queries cluster for the given uuids, searching (by default) for jobs, instances, and groups."""
    count = 0
    entities = {}
    uuids_by_type = entity_refs_to_uuids(cluster, entity_refs)

    # Query for jobs
    job_uuids = uuids_by_type[Types.JOB]
    entities['jobs'] = query_cluster(cluster, job_uuids, pred_jobs, timeout,
                                     interval, make_job_request, Types.JOB)
    count += len(entities['jobs'])

    # Query for instances
    instance_uuids = uuids_by_type[Types.INSTANCE]
    instance_parent_job_pairs = []
    parent_jobs = query_cluster(cluster, instance_uuids, pred_instances, timeout,
                                interval, make_instance_request, Types.INSTANCE)
    for job in parent_jobs:
        for instance in job['instances']:
            if instance['task_id'] in instance_uuids:
                instance_parent_job_pairs.append((instance, job))

    entities['instances'] = instance_parent_job_pairs
    count += len(instance_parent_job_pairs)

    # Query for groups
    group_uuids = uuids_by_type[Types.GROUP]
    entities['groups'] = query_cluster(cluster, group_uuids, pred_groups, timeout,
                                       interval, make_group_request, Types.GROUP)
    count += len(entities['groups'])

    # Update the overall count of entities retrieved
    entities['count'] = count
    return entities


def query_across_clusters(clusters, query_fn):
    """
    Attempts to query entities from the given clusters. The uuids are provided in args. Optionally
    accepts a predicate, pred, which must be satisfied within the timeout.
    """
    count = 0
    all_entities = {'clusters': {}}
    max_workers = os.cpu_count()
    logging.debug('querying with max workers = %s' % max_workers)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_cluster = {query_fn(c, executor): c for c in clusters}
        for future, cluster in future_to_cluster.items():
            entities = future.result()
            all_entities['clusters'][cluster['name']] = entities
            if entities.get('using_pools', False):
                for pool_entities in entities['pools'].values():
                    count += pool_entities['count']
            else:
                count += entities['count']
    all_entities['count'] = count
    return all_entities


def query(clusters, entity_refs, pred_jobs=None, pred_instances=None, pred_groups=None, timeout=None, interval=None):
    """
    Uses query_across_clusters to make the /rawscheduler
    requests in parallel across the given clusters
    """

    def submit(cluster, executor):
        return executor.submit(query_entities, cluster, entity_refs, pred_jobs,
                               pred_instances, pred_groups, timeout, interval)

    return query_across_clusters(clusters, submit)


def no_data_message(clusters):
    """Returns a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    message = terminal.failed(f'No matching data found in {clusters_text}.')
    message = f'{message}\nDo you need to add another cluster to your configuration?'
    return message


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    print(no_data_message(clusters))


def query_unique(clusters, entity_ref):
    """Resolves a uuid to a unique job or (instance, job) pair."""
    query_result = query(clusters, [entity_ref])
    num_results = query_result['count']

    if num_results == 0:
        raise Exception(no_data_message(clusters))

    if num_results > 1:
        # This is unlikely to happen in the wild, but it could.
        # A couple of examples of how this could happen:
        # - same uuid on an instance and a job (not necessarily the parent job)
        # - same uuid on a job in cluster x as another job in cluster y
        raise Exception('There is more than one match for the given uuid.')

    cluster_name, entities = next((c, e) for (c, e) in iter(query_result['clusters'].items()) if e['count'] > 0)
    cluster = next(c for c in clusters if c['name'] == cluster_name)

    # Check for a group, which will raise an Exception
    if len(entities['groups']) > 0:
        raise Exception('You must provide a job uuid or job instance uuid. You provided a job group uuid.')

    # Check for a job
    jobs = entities['jobs']
    if len(jobs) > 0:
        job = jobs[0]
        return {'type': Types.JOB, 'data': job, 'cluster': cluster}

    # Check for a job instance
    instances = entities['instances']
    if len(instances) > 0:
        instance, job = instances[0]
        return {'type': Types.INSTANCE, 'data': (instance, job), 'cluster': cluster}

    # This should not happen (the only entities we generate are jobs, instances, and groups)
    raise Exception(f'Encountered unexpected error when querying for {entity_ref}.')


def __get_latest_instance(job):
    """Returns the most recently started (i.e. latest) instance of the given job"""
    if 'instances' in job:
        instances = job['instances']
        if instances:
            instance = max(instances, key=itemgetter('start_time'))
            return instance

    raise CookRetriableException(f'Job {job["uuid"]} currently has no instances.')


def query_unique_and_run(clusters, entity_ref, command_fn, wait=False):
    """Calls query_unique and then calls the given command_fn on the resulting job instance"""

    def query_unique_and_run():
        query_result = query_unique(clusters, entity_ref)
        if query_result['type'] == Types.JOB:
            job = query_result['data']
            instance = __get_latest_instance(job)
            directory_fn = partial(mesos.retrieve_instance_sandbox_directory, instance=instance, job=job)
            command_fn(job, instance, directory_fn, query_result['cluster'])
        elif query_result['type'] == Types.INSTANCE:
            instance, job = query_result['data']
            directory_fn = partial(mesos.retrieve_instance_sandbox_directory, instance=instance, job=job)
            command_fn(job, instance, directory_fn, query_result['cluster'])
        else:
            # This should not happen, because query_unique should
            # only return a map with type "job" or type "instance"
            raise Exception(f'Encountered error when querying for {entity_ref}.')

    if wait:
        # Importing tenacity locally to prevent startup time
        # from increasing in the default (i.e. don't wait) case
        import tenacity
        one_day_in_seconds = 24 * 60 * 60
        r = tenacity.Retrying(wait=tenacity.wait_fixed(5),
                              retry=tenacity.retry_if_exception_type(CookRetriableException),
                              stop=tenacity.stop_after_delay(one_day_in_seconds),
                              reraise=True)
        r.call(query_unique_and_run)
    else:
        query_unique_and_run()


def resource_to_entity_type(resource):
    """Maps the given resource to the corresponding entity type"""
    resource = resource.lower()
    if resource == 'jobs':
        entity_type = Types.JOB
    elif resource == 'instances':
        entity_type = Types.INSTANCE
    elif resource == 'groups':
        entity_type = Types.GROUP
    else:
        raise Exception(f'{resource} refers to an unsupported resource.')

    return entity_type


def cluster_url_to_name(cluster_url, clusters):
    """
    Given a cluster URL and the configured clusters, returns the
    corresponding cluster name, or throws if none is found
    """
    matched_clusters = [c for c in clusters if c['url'].lower().rstrip('/') == cluster_url.lower()]
    if len(matched_clusters) == 0:
        raise Exception(f'There is no configured cluster that matches {cluster_url}.')

    cluster_name = matched_clusters[0]['name']
    return cluster_name


def parse_entity_ref(ref_string, cluster_url_to_name_fn):
    """
    Returns a list of entity ref maps, where each map has the following shape:

      {'cluster': ..., 'type': ..., 'uuid': ...}

    The cluster field is the name of a cluster, the type field can be either job, instance, or group,
    and the uuid field is the uuid of the entity in question.

    An entity ref string can either be simply a UUID, in which case all configured clusters and all
    three types will be queried, or it can be a Cook Scheduler URL that represents the way to retrieve
    that specific entity.

    Some examples of valid entity ref strings follow:

      5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster2.example.com/jobs/5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster2.example.com/jobs?uuid=5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster3.example.com/instances/5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster3.example.com/instances?uuid=5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster4.example.com/groups/5ab383b1-5b8b-4483-b2b2-45126923f4df
      http://cluster4.example.com/groups?uuid=5ab383b1-5b8b-4483-b2b2-45126923f4df

    Throws if an invalid entity ref string is encountered.
    """
    result = urlparse(ref_string)

    if not result.path:
        raise Exception(f'{ref_string} is not a valid entity reference.')

    if not result.netloc:
        if not is_valid_uuid(result.path):
            raise Exception(f'{result.path} is not a valid UUID.')

        return [{'cluster': Clusters.ALL, 'type': Types.ALL, 'uuid': result.path}]

    path_parts = result.path.split('/')
    num_path_parts = len(path_parts)
    cluster_url = (f'{result.scheme}://' if result.scheme else '') + result.netloc

    if num_path_parts < 2:
        raise Exception(f'Unable to determine entity type and UUID from {ref_string}.')

    if num_path_parts == 2 and not result.query:
        raise Exception(f'Unable to determine UUID from {ref_string}.')

    cluster_name = cluster_url_to_name_fn(cluster_url)
    entity_type = resource_to_entity_type(path_parts[1])

    if num_path_parts > 2:
        return [{'cluster': cluster_name, 'type': entity_type, 'uuid': path_parts[2]}]

    query_args = parse_qs(result.query)

    if 'uuid' not in query_args:
        raise Exception(f'Unable to determine UUID from {ref_string}.')

    return [{'cluster': cluster_name, 'type': entity_type, 'uuid': uuid} for uuid in query_args['uuid']]


def parse_entity_refs(clusters, ref_strings):
    """
    Given the collection of configured clusters and a collection of entity ref strings, returns a pair
    where the first element is a list of corresponding entity ref maps, and the second element is the
    subset of clusters that are of interest.
    """
    entity_refs = []
    cluster_names_of_interest = set()
    all_cluster_names = set(c['name'] for c in clusters)

    for ref_string in ref_strings:
        parsed_refs = parse_entity_ref(ref_string, partial(cluster_url_to_name, clusters=clusters))
        entity_refs.extend(parsed_refs)
        for entity_ref in parsed_refs:
            cluster_name = entity_ref['cluster']
            if cluster_name == Clusters.ALL:
                cluster_names_of_interest = all_cluster_names
            else:
                cluster_names_of_interest.add(cluster_name)

    clusters_of_interest = [c for c in clusters if c['name'] in cluster_names_of_interest]
    return entity_refs, clusters_of_interest


def query_with_stdin_support(clusters, entity_refs, pred_jobs=None, pred_instances=None,
                             pred_groups=None, timeout=None, interval=None):
    """
    Queries for UUIDs across clusters, supporting input being passed via stdin, e.g.:

      $ cs jobs --user sally --running --waiting -1 | cs wait

    The above example would wait for all of sally's running and waiting jobs to complete. Returns a pair where the
    first element is the query result map, and the second element is the subset of clusters that are of interest.
    """
    is_stdin_from_pipe = not sys.stdin.isatty()
    text_read_from_pipe = sys.stdin.read() if is_stdin_from_pipe else None

    if entity_refs and text_read_from_pipe:
        raise Exception(f'You cannot supply entity references both as arguments and from stdin.')

    clusters_of_interest = clusters
    if not entity_refs:
        if is_stdin_from_pipe:
            text = text_read_from_pipe
        else:
            print_info('Enter the UUIDs or URLs, one per line (press Ctrl+D on a blank line to submit)')
            text = sys.stdin.read()

        if not text:
            raise Exception('You must specify at least one UUID or URL.')

        ref_strings = text.splitlines()
        entity_refs, clusters_of_interest = parse_entity_refs(clusters, ref_strings)

    query_result = query(clusters_of_interest, entity_refs, pred_jobs, pred_instances, pred_groups, timeout, interval)
    return query_result, clusters_of_interest

def get_compute_cluster_config(cluster, compute_cluster_name):
    """
    :param cluster: cook scheduler cluster
    :param compute_cluster_name: compute cluster
    :return: config of the compute cluster Looks at both /settings (for static clusters) and /compute-clusters (for dynamic clusters)
    """
    cook_cluster_settings = http.get(cluster, 'settings', params={}).json()
    cook_compute_clusters = http.get(cluster, 'compute-clusters', params={}).json()
    rval = next((c['cluster-definition']['config'] for c in cook_compute_clusters['in-mem-configs'] if
                 c['name'] == compute_cluster_name), None)
    if not rval:
        rval = next((c for c in (s['config'] for s in cook_cluster_settings['compute-clusters']) if
                     c['compute-cluster-name'] == compute_cluster_name), None)
    return rval
