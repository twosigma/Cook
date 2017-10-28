import concurrent
import logging
import os
from concurrent import futures
from operator import itemgetter

from cook import http, colors, progress, mesos
from cook.util import wait_until


def query_cluster(cluster, uuids, pred, timeout, interval, make_request_fn, entity_type):
    """
    Queries the given cluster for the given uuids with
    an optional predicate, pred, that must be satisfied
    """

    def satisfy_pred():
        return pred(http.make_data_request(cluster, lambda: make_request_fn(cluster, uuids)))

    entities = http.make_data_request(cluster, lambda: make_request_fn(cluster, uuids))
    if pred and len(entities) > 0:
        if entity_type == 'job':
            wait_text = 'Waiting for the following jobs'
        elif entity_type == 'instance':
            wait_text = 'Waiting for instances of the following jobs'
        elif entity_type == 'group':
            wait_text = 'Waiting for the following job groups'
        else:
            raise Exception('Invalid entity type %s.' % entity_type)

        uuid_text = ', '.join([e['uuid'] for e in entities])
        wait_text = '%s on %s: %s' % (wait_text, colors.bold(cluster['name']), uuid_text)
        index = progress.add(wait_text)
        if pred(entities):
            progress.update(index, colors.bold('Done'))
        else:
            entities = wait_until(satisfy_pred, timeout, interval)
            if entities:
                progress.update(index, colors.bold('Done'))
            else:
                raise TimeoutError('Timeout waiting for response.')
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


def query_entities(cluster, uuids, pred_jobs, pred_instances, pred_groups, timeout, interval,
                   include_jobs=True, include_instances=True, include_groups=True):
    """Queries cluster for the given uuids, searching (by default) for jobs, instances, and groups."""
    count = 0
    entities = {}
    if include_jobs:
        entities['jobs'] = query_cluster(cluster, uuids, pred_jobs, timeout,
                                         interval, make_job_request, 'job')
        count += len(entities['jobs'])
    if include_instances:
        entities['instances'] = query_cluster(cluster, uuids, pred_instances, timeout,
                                              interval, make_instance_request, 'instance')
        count += len(entities['instances'])
    if include_groups:
        entities['groups'] = query_cluster(cluster, uuids, pred_groups, timeout,
                                           interval, make_group_request, 'group')
        count += len(entities['groups'])
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
            count += entities['count']
    all_entities['count'] = count
    return all_entities


def query(clusters, uuids, pred_jobs=None, pred_instances=None, pred_groups=None, timeout=None, interval=None):
    """
    Uses query_across_clusters to make the /rawscheduler
    requests in parallel across the given clusters
    """
    # Cook will give us back two copies if the user asks for the same UUID twice, e.g.
    # $ cs show d38ea6bd-8a26-4ddf-8a93-5926fa2991ce d38ea6bd-8a26-4ddf-8a93-5926fa2991ce
    # Prevent this by creating a set:
    uuids = set(uuids)

    def submit(cluster, executor):
        return executor.submit(query_entities, cluster, uuids, pred_jobs,
                               pred_instances, pred_groups, timeout, interval)

    return query_across_clusters(clusters, submit)


def no_data_message(clusters):
    """Returns a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    message = colors.failed(f'No matching data found in {clusters_text}.')
    message = f'{message}\nDo you need to add another cluster to your configuration?'
    return message


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    print(no_data_message(clusters))


def query_unique(clusters, uuid):
    """Resolves a uuid to a unique job or (instance, job) pair."""
    query_result = query(clusters, [uuid])
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

    # Check for a group, which will raise an Exception
    if len(entities['groups']) > 0:
        raise Exception('You must provide a job uuid or job instance uuid. You provided a job group uuid.')

    # Check for a job
    jobs = entities['jobs']
    if len(jobs) > 0:
        job = jobs[0]
        return {'type': 'job', 'data': job}

    # Check for a job instance
    instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] == uuid]
    if len(instances) > 0:
        instance, job = instances[0]
        return {'type': 'instance', 'data': (instance, job)}

    # This should not happen (the only entities we generate are jobs, instances, and groups)
    raise Exception(f'Encountered unexpected error when querying for uuid {uuid}.')


def __get_latest_instance(job):
    """Returns the most recently started (i.e. latest) instance of the given job"""
    if 'instances' in job:
        instances = job['instances']
        if instances:
            instance = max(instances, key=itemgetter('start_time'))
            return instance

    raise Exception(f'Job {job["uuid"]} currently has no instances.')


def query_unique_and_run(clusters, job_or_instance_uuid, command_fn):
    """Calls query_unique and then calls the given command_fn on the resulting job instance"""
    query_result = query_unique(clusters, job_or_instance_uuid)
    if query_result['type'] == 'job':
        job = query_result['data']
        instance = __get_latest_instance(job)
        directory = mesos.retrieve_instance_sandbox_directory(instance, job)
        command_fn(instance, directory)
    elif query_result['type'] == 'instance':
        instance, job = query_result['data']
        directory = mesos.retrieve_instance_sandbox_directory(instance, job)
        command_fn(instance, directory)
    else:
        # This should not happen, because query_unique should
        # only return a map with type "job" or type "instance"
        raise Exception(f'Encountered error when querying for {job_or_instance_uuid}.')
