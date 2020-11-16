import collections
import itertools
import json

from tabulate import tabulate

from cook.format import format_instance_run_time, format_instance_status, format_job_memory, format_list, format_dict, \
    format_job_attempts, format_job_status
from cook.querying import print_no_data, parse_entity_refs, query_with_stdin_support
from cook.util import millis_to_timedelta, millis_to_date_string, guard_no_cluster

DEFAULT_MAX_RUNTIME = 2 ** 63 - 1


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


def tabulate_job(cluster_name, job):
    """Given a job, returns a string containing tables for the job and instance fields"""
    job_definition = [['Cluster', cluster_name],
                      ['Pool', job.get('pool', '-')],
                      ['Memory', format_job_memory(job)],
                      ['CPUs', job['cpus']],
                      ['User', job['user']],
                      ['Priority', job['priority']]]
    if job['max_runtime'] != DEFAULT_MAX_RUNTIME:
        job_definition.append(['Max Runtime', millis_to_timedelta(job['max_runtime'])])
    if job['gpus'] > 0:
        job_definition.append(['GPUs', job['gpus']])
    if 'disk' in job:
        job_definition.append(['Disk Request', job['disk']['request']])
        if 'limit' in job['disk']:
            job_definition.append(['Disk Limit', job['disk']['limit']])
        if 'type' in job['disk']:
            job_definition.append(['Disk Type', job['disk']['type']])
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
    if 'executor' in job:
        job_definition.append(['Executor', job['executor']])

    job_state = [['Attempts', format_job_attempts(job)],
                 ['Job Status', format_job_status(job)],
                 ['Submitted', millis_to_date_string(job['submit_time'])]]
    if 'checkpoint' in job and 'mode' in job['checkpoint']:
        job_state.append(['Checkpoint mode', job['checkpoint']['mode']])

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
            ['Agent', instance['slave_id']],
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


def show(clusters, args, _):
    """Prints info for the jobs / instances / groups with the given UUIDs."""
    guard_no_cluster(clusters)
    as_json = args.get('json')
    entity_refs, _ = parse_entity_refs(clusters, args.get('uuid'))
    query_result, clusters_of_interest = query_with_stdin_support(clusters, entity_refs)
    if as_json:
        print(json.dumps(query_result))
    else:
        for cluster_name, entities in query_result['clusters'].items():
            if 'jobs' in entities:
                show_data(cluster_name, entities['jobs'], tabulate_job)

            if 'instances' in entities:
                show_data(cluster_name, entities['instances'], tabulate_instance)

            if 'groups' in entities:
                show_data(cluster_name, entities['groups'], tabulate_group)

    if query_result['count'] > 0:
        return 0
    else:
        if not as_json:
            print_no_data(clusters_of_interest)
        return 1


def register(add_parser, _):
    """Adds this sub-command's parser and returns the action function"""
    show_parser = add_parser('show', help='show jobs / instances / groups by uuid')
    show_parser.add_argument('uuid', nargs='*')
    show_parser.add_argument('--json', help='show the data in JSON format', dest='json', action='store_true')
    return show
