import collections
import concurrent
import itertools
import json
import logging
import os
import time
from concurrent import futures

import humanfriendly
from tabulate import tabulate

from cook import colors, progress, http
from cook.util import strip_all, wait_until, print_info, millis_to_timedelta, millis_to_date_string

DEFAULT_MAX_RUNTIME = 2 ** 63 - 1


def format_dict(d):
    """Formats the given dictionary for display in a table"""
    return ' '.join(['%s=%s' % (k, v) for k, v in sorted(d.items())]) if len(d) > 0 else '(empty)'


def format_list(l):
    """Formats the given list for display in a table"""
    return '; '.join([format_dict(x) if isinstance(x, dict) else str(x) for x in l]) if len(l) > 0 else '(empty)'


def format_state(state):
    """Capitalizes and colorizes the given state"""
    state = state.capitalize()
    if state == 'Running':
        text = colors.running(state)
    elif state == 'Waiting':
        text = colors.waiting(state)
    elif state == 'Failed':
        text = colors.failed(state)
    elif state == 'Success':
        text = colors.success(state)
    else:
        text = state
    return text


def format_instance_status(instance):
    """Formats the instance status field"""
    status_text = format_state(instance['status'])
    if 'reason_string' in instance:
        parenthetical_text = ' (%s)' % colors.reason(instance['reason_string'])
    elif 'progress' in instance and instance['progress'] > 0:
        parenthetical_text = ' (%s%%)' % instance['progress']
    else:
        parenthetical_text = ''

    return '%s%s' % (status_text, parenthetical_text)


def format_instance_run_time(instance):
    """Formats the instance run time field"""
    if 'end_time' in instance:
        end = instance['end_time']
    else:
        end = int(round(time.time() * 1000))
    run_time = millis_to_timedelta(end - instance['start_time'])
    return '%s (started %s)' % (run_time, millis_to_date_string(instance['start_time']))


def tabulate_job_instances(instances):
    """Returns a table displaying the instance info"""
    if len(instances) > 0:
        rows = [collections.OrderedDict([('Job Instance', i['task_id']),
                                         ('Run Time', format_instance_run_time(i)),
                                         ('Host', i['hostname']),
                                         ('Instance Status', format_instance_status(i))])
                for i in instances]
        instance_table = tabulate(rows, headers='keys', tablefmt='plain')
        return '\n\n%s' % instance_table
    else:
        return ''


def juxtapose_text(text_a, text_b, buffer_len=15):
    """Places text_a to the left of text_b with a buffer of spaces in between"""
    lines_a = text_a.splitlines()
    lines_b = text_b.splitlines()
    longest_line_length_a = max(map(len, lines_a))
    paired_lines = itertools.zip_longest(lines_a, lines_b, fillvalue="")
    a_columns = longest_line_length_a + buffer_len
    return "\n".join("{0:<{1}}{2}".format(a, a_columns, b) for a, b in paired_lines)


def format_job_status(job):
    """Formats the job status field"""
    return format_state(job['state'])


def format_job_memory(job):
    """Formats the job memory field"""
    return humanfriendly.format_size(job['mem'] * 1000 * 1000)


def format_job_attempts(job):
    """Formats the job attempts field (e.g. 2 / 5)"""
    return '%s / %s' % (job['max_retries'] - job['retries_remaining'], job['max_retries'])


def tabulate_job(cluster_name, job):
    """Given a job, returns a string containing tables for the job and instance fields"""
    job_definition = [['Cluster', cluster_name],
                      ['Memory', format_job_memory(job)],
                      ['CPUs', job['cpus']],
                      ['User', job['user']],
                      ['Priority', job['priority']]]
    if job['max_runtime'] != DEFAULT_MAX_RUNTIME:
        job_definition.append(['Max Runtime', millis_to_timedelta(job['max_runtime'])])
    if job['gpus'] > 0:
        job_definition.append(['GPUs', job['gpus']])
    if job['ports'] > 0:
        job_definition.append(['Ports Requested', job['ports']])
    if len(job['constraints']) > 0:
        job_definition.append(['Constraints', format_list(job['constraints'])])
    if len(job['labels']) > 0:
        job_definition.append(['Labels', format_dict(job['labels'])])
    if job['uris'] and len(job['uris']) > 0:
        job_definition.append(['URI(s)', format_list(job['uris'])])
    if 'groups' in job:
        job_definition.append(['Job Group(s)', format_list(job['groups'])])
    if 'application' in job:
        job_definition.append(['Application', '%s (%s)' % (job['application']['name'], job['application']['version'])])

    job_state = [['Attempts', format_job_attempts(job)],
                 ['Job Status', format_job_status(job)],
                 ['Submitted', millis_to_date_string(job['submit_time'])]]

    job_command = 'Command:\n%s' % job['command']

    if len(job['env']) > 0:
        environment = '\n\nEnvironment:\n%s' % '\n'.join(['%s=%s' % (k, v) for k, v in job['env'].items()])
    else:
        environment = ''

    instances = job['instances']

    job_definition_table = tabulate(job_definition, tablefmt='plain')
    job_state_table = tabulate(job_state, tablefmt='plain')
    job_tables = juxtapose_text(job_definition_table, job_state_table)
    instance_table = tabulate_job_instances(instances)
    return '\n=== Job: %s (%s) ===\n\n%s\n\n%s%s%s' % \
           (job['uuid'], job['name'], job_tables, job_command, environment, instance_table)


def tabulate_instance(cluster_name, instance_job_pair):
    """Given an instance, returns a string containing a table for the instance fields"""
    instance = instance_job_pair[0]
    job = instance_job_pair[1]

    left = [['Cluster', cluster_name],
            ['Host', instance['hostname']],
            ['Slave', instance['slave_id']],
            ['Job', '%s (%s)' % (job['name'], job['uuid'])]]
    if len(instance['ports']) > 0:
        left.append(['Ports Allocated', format_list(instance['ports'])])

    right = [['Run Time', format_instance_run_time(instance)],
             ['Instance Status', format_instance_status(instance)],
             ['Job Status', format_job_status(job)]]
    if 'exit_code' in instance:
        right.append(['Exit Code', instance['exit_code']])

    left_table = tabulate(left, tablefmt='plain')
    right_table = tabulate(right, tablefmt='plain')
    instance_tables = juxtapose_text(left_table, right_table)
    return '\n=== Job Instance: %s ===\n\n%s' % (instance['task_id'], instance_tables)


def tabulate_group(cluster_name, group):
    """Given a group, returns a string containing a table for the group fields"""
    left = [['Cluster', cluster_name]]
    if group['host_placement']['type'] == 'all':
        left.append(['Host Placement', 'all hosts'])
    else:
        left.append(['Host Placement', format_dict(group['host_placement'])])
    if group['straggler_handling']['type'] == 'none':
        left.append(['Straggler Handling', 'none'])
    else:
        left.append(['Straggler Handling', format_dict(group['straggler_handling'])])

    right = [['# Completed', group['completed']],
             ['# Running', group['running']],
             ['# Waiting', group['waiting']]]

    num_jobs = len(group['jobs'])
    jobs = 'Job group contains %s job%s:\n%s' % (num_jobs, '' if num_jobs == 1 else 's', '\n'.join(group['jobs']))

    left_table = tabulate(left, tablefmt='plain')
    right_table = tabulate(right, tablefmt='plain')
    group_tables = juxtapose_text(left_table, right_table)
    return '\n=== Job Group: %s (%s) ===\n\n%s\n\n%s' % (group['uuid'], group['name'], group_tables, jobs)


def show_data(cluster_name, data, format_fn):
    """Iterates over the data collection and formats and prints each element"""
    count = len(data)
    if count > 0:
        tables = [format_fn(cluster_name, datum) for datum in data]
        output = '\n\n'.join(tables)
        print(output)
        print()
    return count


def query_cluster(cluster, uuids, pred, timeout, interval, make_request_fn, entity_type):
    """
    Queries the given cluster for the given uuids with
    an optional predicate, pred, that must be satisfied
    """

    def satisfy_pred():
        return pred(http.make_data_request(lambda: make_request_fn(cluster, uuids)))

    entities = http.make_data_request(lambda: make_request_fn(cluster, uuids))
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

    def submit(cluster, executor):
        return executor.submit(query_entities, cluster, uuids, pred_jobs,
                               pred_instances, pred_groups, timeout, interval)

    return query_across_clusters(clusters, submit)


def print_no_data(clusters):
    """Prints a message indicating that no data was found in the given clusters"""
    clusters_text = ' / '.join([c['name'] for c in clusters])
    print(colors.failed('No matching data found in %s.' % clusters_text))
    print_info('Do you need to add another cluster to your configuration?')


def show(clusters, args):
    """Prints info for the jobs / instances / groups with the given UUIDs."""
    as_json = args.get('json')
    uuids = strip_all(args.get('uuid'))
    query_result = query(clusters, uuids)
    if as_json:
        print(json.dumps(query_result))
    else:
        for cluster_name, entities in query_result['clusters'].items():
            jobs = entities['jobs']
            instances = [[i, j] for j in entities['instances'] for i in j['instances'] if i['task_id'] in uuids]
            groups = entities['groups']
            show_data(cluster_name, jobs, tabulate_job)
            show_data(cluster_name, instances, tabulate_instance)
            show_data(cluster_name, groups, tabulate_group)
    if query_result['count'] > 0:
        return 0
    else:
        if not as_json:
            print_no_data(clusters)
        return 1


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('show', help='show jobs / instances / groups by uuid')
    show_parser.add_argument('uuid', nargs='+')
    show_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    return show
