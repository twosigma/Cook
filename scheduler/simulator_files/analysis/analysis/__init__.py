import matplotlib
import matplotlib.cm as cmx
import matplotlib.colors
import matplotlib.pyplot as plt
import numpy as np
import pandas



def prepare_df(df):
    """Reads an output trace file into a dataframe and translates the time columns 
    so the minimum time occurring is 0
    
    Parameters:
    -----------
    out_trace_df : pandas.DataFrame
        dataframe of csv output from simulation run.
        Expected columns:
        [submit_time_ms, start_time_ms, end_time_ms, mesos_start_time_ms]
        
    Returns:
    -------
    df : pandas.DataFrame
        dataframe containing the fields from the output trace file"""
    min_time = min(df["submit_time_ms"])
    df["start_time_ms"] =df["start_time_ms"]-min_time
    df["end_time_ms"] =df["end_time_ms"]-min_time
    df["submit_time_ms"] = df["submit_time_ms"]-min_time
    df["mesos_start_time_ms"] = df["mesos_start_time_ms"] - min_time
    df["run_time_ms"] = df.end_time_ms - df.start_time_ms
    df.loc[df['run_time_ms'] < 0, 'end_time_ms'] = df.end_time_ms.max()
    df["run_time_ms"] = df.end_time_ms - df.start_time_ms
    df["overhead"] = df.start_time_ms - df.submit_time_ms
    return df

def job_view_stats(df):
    """Produces a dataframe that is focused on the job level. 
    Each row is a job, the task id is the last task that ran and 'overhead'
    is (end_time_ms-submit_time_ms)-run_time_ms. 
    This measures the added cost from the scheduler and lack of infinite compute.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        dataframe returned from prepare_df
        
    Returns:
    --------
    df : pandas.DataFrame
        dataframe with job level info"""
    df = df.copy()
    df = df.sort_values("end_time_ms").groupby("job_id").last()
    df["submit_to_complete_ms"] = df.end_time_ms - df.submit_time_ms
    df["job_overhead"] = df.submit_to_complete_ms - df.run_time_ms
    df.reset_index(inplace=True)
    return df

def running_tasks_at(df, t):
    """Returns the tasks running at t
    
    Parameters:
    -----------
    df : pandas.DataFrame
        prepare_df outputted dataframe
    t : int
        time in milliseconds (in translated time)
        
    Returns:
    --------
    pandas.DataFrame
        dataframe with same form as df, but only with tasks that were running at t"""
    df = df.copy()
    started_before_df = df[df.start_time_ms <= t]
    completed_after_df = started_before_df[started_before_df.end_time_ms > t]
    return completed_after_df
    
def point_in_time_analysis(df, t):
    """Returns the 5 dataframes with analysis for a particular point in time
    
    Parameters:
    -----------
    df : pandas.DataFrame
        prepare_df outputted dataframe
    t : int
        time in milliseconds (in translated time)
        
    Returns:
    --------
    per_host : pandas.DataFrame
        dataframe with a row per host. Has column for mem, cpus and count 
        of tasks running on the host
    per_user : pandas.DataFrame
        dataframe with a row per user. Has a column for mem, cpus and count
        of tasks running for the user
    waiting : pandas.DataFrame
        dataframe with a row for tasks that were submitted before t but were 
        not running at t. 
    running_tasks_df : pandas.DataFrame
        dataframe outputted by running_tasks_at
    df : pandas.DataFrame
        input dataframe
        """
    running_df = running_tasks_at(df, t)
    running_df["count"] = 1
    per_host = running_df.groupby("hostname").sum()[["mem", "cpus", "count"]].reset_index()
    per_user = running_df.groupby("user").sum()[["mem", "cpus", "count"]].reset_index()
    waiting = df[(df.submit_time_ms < t) & (df.start_time_ms > t)]
    return [per_host, per_user, waiting, running_df, df]


def time_series_events(events):
    """Given a list of event tuples (time, count, mem, cpus),
    produce a time series of the cumulative sum of each of values at each
    time
    
    Parameters:
    -----------
    events : list of tuples (time, count, mem, cpus)
    
    Returns:
    ---------
    pandas.DataFrame
         dataframe time series of usage with keys 
         'time_ms', 'count', 'cpus', 'mem'
         where 
         1. 'count' is the number of tasks
         2. 'mem' is the memory  
         3. 'cpus' is the cpus
         at time 'time_ms'.
    """
    ordered_events = sorted(events, key=lambda x: x[0])
    time_series = []
    count_total = 0
    mem_total = 0
    cpus_total = 0
    for time, count, mem, cpus in ordered_events:
        count_total += count
        mem_total += mem
        cpus_total += cpus
        time_series.append({"time_ms" : time, "count" : count_total, "cpus" : cpus_total, "mem": mem_total})
    return pandas.DataFrame(time_series)


def running_concurrently(df):
    """Given a dataframe of tasks, returns a dataframe where each row 
    is the utilization at a given time
    
     
     Parameters:
     -----------
     df : pandas.DataFrame
         dataframe output from prepare_df
         
     Returns:
     --------
     pandas.DataFrame
         dataframe time series of utilization with keys 
         'time_ms', 'count', 'cpus', 'mem'
         where 
         1. 'count' is the number of tasks running
         2. 'mem' is the memory utilized 
         3. 'cpus' is the cpus utilized
         at time 'time_ms'."""
    rows = df.to_records()
    events = [e for r in rows for e in [(r["start_time_ms"], 1, r["mem"], r["cpus"]), 
                                        (r["end_time_ms"], -1, -r["mem"], -r["cpus"])]]
    return time_series_events(events)


def waiting_over_time(df):
    """Returns a time series of count, mem and cpus waiting
    
    Parameters:
     -----------
     df : pandas.DataFrame
         dataframe output from prepare_df
         
     Returns:
     --------
     pandas.DataFrame
         dataframe time series of waiting with keys 
         'time_ms', 'count', 'cpus', 'mem'
         where 
         1. 'count' is the number of tasks waiting
         2. 'mem' is the memory waiting 
         3. 'cpus' is the cpus waiting
         at time 'time_ms'."""
    rows = df.to_records()
    events = [e for r in rows for e in [(r["submit_time_ms"], 1, r["mem"], r["cpus"]), 
                                        (r["start_time_ms"], -1, -r["mem"], -r["cpus"])]]
    return time_series_events(events)


def mem_tb_hours_run(df):
    return (df.mem*df.run_time_ms).sum()/(1000*60*60)/(1024*1024)


def cpu_hours_run(df):
    return (df.cpus*df.run_time_ms).sum()/(1000*60*60) 

def sample_usage(user_running_over_time, user_waiting_over_time, cycle_time_ms):
    """Samples the usages at fixed time step for both running and waiting

    Parameters:
    -----------
    user_running_over_time : pandas.DataFrame
        A dataframe with columns:
        ['time_ms', 'user', 'mem', 'cpus', 'count']

    user_waiting_over_time : pandas.DataFrame
        A dataframe with columns:
        ['time_ms', 'user', 'mem', 'cpus', 'count']
    cycle_time_ms : int
        time step to sample at

    Returns:
    --------
    dataframe with a row per user per time_ms with the colums from both 
    running and waiting of the most recent update for that user time_ms pair
    prior to the time_ms. waiting columns will have no suffix, running 
    columns will have a _running suffix

    """
    clock = range(0, user_running_over_time.time_ms.max(), cycle_time_ms)
    users = np.unique(user_running_over_time.user.values)
    rows = []
    for tick in clock:
        for user in users:
            rows.append({"time_ms" : tick, "user" : user})
    df = pandas.DataFrame(rows)
    df = pandas.merge_asof(df, user_waiting_over_time, 
                           on="time_ms", by="user", 
                           suffixes=("", "_waiting"))
    df = pandas.merge_asof(df, user_running_over_time, 
                           on="time_ms", by="user",
                           suffixes=("", "_running"))
    return df


def add_starvation(df_with_fairness):
    """Returns a data frame with a column `starved_mem_gb` which is the amount of gbs a user is starved.
    Starved is defined as amount of resources a user wants, below their share, that they aren't getting
    
    Parameters:
    -----------
    df_with_fairness : pandas.DataFrame
        A dataframe of usage (from intervalize usage) annotated with the fair allocation for each user at
        each time slice
        
    Returns:
    --------
    pandas.DataFrame
        df_with_fairness annotated with a column `starved_mem_gb`
    """
    df = df_with_fairness
    df["share_mem"] = 2.5e6
    df["starved_mem_gb"] = 0
    df.loc[df["mem_running"] < df.share_mem, "starved_mem_gb"] = (df[["mem_fair", "share_mem"]].min(axis=1) - df.mem_running)/1000
    df.loc[df["starved_mem_gb"] < 0, "starved_mem_gb"] = 0 # starved is on interval [0,share_mem]
    return df

def prepare_desired_resource_dicts(point_in_time_usage):
    """Given a point in time usage, returns a list of dicts with keys columns `mem` and `user` which is the 
    sum of the waiting and running memory for each user
       
       Parameters:
       -----------
       point_in_time_usage: pandas.DataFrame
           A dataframe with keys: [time_ms, user, mem, count, mem_running, count_running]
           
        Returns:
        --------
        desired_resources: list of dicts
            A list of dicts with keys: [user, mem] which provides the user's desired resources
            sorted by mem"""
    user_desired = point_in_time_usage.copy()
    user_desired.mem += user_desired.mem_running
    user_desired = user_desired[["mem", "user"]]
    user_desired.dropna()
    user_desired = user_desired[(user_desired.mem>0)]
    resource_records = user_desired.sort_values("mem").to_records()
    desired_resources = [{"mem" : mem, "user" : user} for _,mem,user in resource_records]
    return desired_resources

def get_fair_allocation(point_in_time_usage):
    """Given a point in time usage, returns the fair allocation for each user with the following assumptions:
       1. We only care about memory
       2. All user shares are equal
       Generalizing this shouldn't be TOO difficult
       
       Parameters:
       -----------
       point_in_time_usage: pandas.DataFrame
           A dataframe with keys: [time_ms, user, mem, count, mem_running, count_running]
           
        Returns:
        --------
        allocations_df: pandas.DataFrame
            A dataframe with keys: [user, mem] which provides the users fair allocation given what they requested"""
    mem_to_allocate = point_in_time_usage.mem_running.sum()
    desired_resources = prepare_desired_resource_dicts(point_in_time_usage)
    allocations = {user : 0 for user in point_in_time_usage.user}
    mem_allocated_per_active_user = 0
    
    while mem_to_allocate > 1e-6:
        min_mem = desired_resources[0]["mem"] - mem_allocated_per_active_user
        if mem_to_allocate < min_mem * len(desired_resources):
            min_mem = mem_to_allocate/len(desired_resources)
    
        mem_allocated_per_active_user += min_mem
    
        non_pos_count = 0

        while non_pos_count < len(desired_resources) and (desired_resources[non_pos_count]["mem"] - mem_allocated_per_active_user - 1e-6) <= 0:
            allocations[desired_resources[non_pos_count]["user"]] = mem_allocated_per_active_user
            non_pos_count += 1
        
        mem_to_allocate -= min_mem*len(desired_resources)
        desired_resources = desired_resources[non_pos_count:] 

    # Any users remaining at the end still had resources desired resources. Give them the full allocation 
    for user_mem_desired in desired_resources:
        allocations[user_mem_desired["user"]] = mem_allocated_per_active_user

    allocation_df = pandas.DataFrame([(user,mem) for user,mem in allocations.items()],
                                     columns=['user','mem'])
    allocation_df["time_ms"] = point_in_time_usage.time_ms.values[0] #they will all be the same
    return allocation_df

def prepare_usage_df(user_running, user_waiting, cycle_time):
    usage_df = sample_usage(user_running, user_waiting, cycle_time)
    fair_allocs = usage_df.groupby("time_ms", as_index=False).apply(get_fair_allocation).reset_index(drop=True)
    usage_df = pandas.merge(usage_df, fair_allocs, on=["time_ms", "user"], suffixes=("","_fair"))
    usage_df = add_starvation(usage_df)
    usage_df["fair_minus_running"] = np.abs(usage_df.mem_fair - usage_df.mem_running)
    usage_df["fair_ratio"] = usage_df.mem_running/usage_df.mem_fair
    usage_df["starved_mem_log10"] = np.log10(usage_df[usage_df.starved_mem_gb > 0].starved_mem_gb)
    return usage_df

def score_card(task_df, user_running, user_waiting, cycle_time):
    """Returns a dataframe where each column is a different metric. 
    Whether a larger values or lower values is better is noted where it makes sense.
    
    Metrics:
    
    "cpu_hours" : total cpu hours used by tasks, up is good
    "cpu_hours_preemption" : cpu hours for jobs that were preempted, down is good
    "cpu_hours_success" : cpu hours for jobs that were successful, up is good

    "mem_running_over_fair_alloc_median" : measure of fairness. median underallocation over time and users. up is good  

    "mem_tb_hours" : total memory hours in tb used by tasks, up is good
    "mem_tb_hours_preemption" : memory hours in tb for jobs that were preempted, down is good
    "mem_tb_hours_success" : memory hours in tb for jobs that were successful, up is good
    
    "scheduling_latency_mean" : mean scheduling latency for all tasks, down is good
    "scheduling_latency_mean_preemption" : mean scheduling latency for preempted tasks, down is good
    "scheduling_latency_mean_success" : mean scheduling latency for successfully tasks, down is good
    
    "scheduling_latency_p50" : median scheduling latency for all tasks, down is good
    "scheduling_latency_p50_preemption" : median scheduling latency for preempted tasks, down is good
    "scheduling_latency_p50_success" : median scheduling latency for successfully tasks, down is good

    "starvation_log10_median": median of log 10 starvation across all users and time. down is good
    "starvation_log10_mean_cycle_median" : median of mean of log 10 starvation across users for each time. down is good

    "tasks_run" : total number of tasks run, up is good 
    "task_run_preemption" : tasks that were run that failed due to preemption, down is good
    "tasks_run_success" : tasks that were run that succeeded, up is good
    """
    task_df.reason = task_df.reason.astype(str)
    success_task_df = task_df[task_df.status == ":instance.status/success"]
    preemption_task_df = task_df[task_df.reason == "Preempted by rebalancer"]
    running_task_df = task_df[task_df.status == ":instance.status/running"]
    usage_df = prepare_usage_df(user_running, user_waiting, cycle_time)
    values = [{"cpu_hours" : cpu_hours_run(task_df),
               "cpu_hours_preemption" : cpu_hours_run(preemption_task_df),
               "cpu_hours_success" : cpu_hours_run(success_task_df),
               "cpu_hours_running" : cpu_hours_run(running_task_df),
               
               "mem_running_over_fair_alloc_median" : usage_df[(usage_df.fair_ratio > 0) & (usage_df.fair_ratio <= 1)].fair_ratio.median(),
                             
               "mem_tb_hours" : mem_tb_hours_run(task_df),
               "mem_tb_hours_preemption" : mem_tb_hours_run(preemption_task_df),
               "mem_tb_hours_success" : mem_tb_hours_run(success_task_df),
               "mem_tb_hours_running" : mem_tb_hours_run(running_task_df),
               
               "scheduling_latency_mean" : task_df.overhead.mean(),
               "scheduling_latency_mean_preemption" : preemption_task_df.overhead.mean(), 
               "scheduling_latency_mean_success" : success_task_df.overhead.mean(),
               
               "scheduling_latency_p50" : task_df.overhead.median(),
               "scheduling_latency_p50_preemption" : preemption_task_df.overhead.median(), 
               "scheduling_latency_p50_success" : success_task_df.overhead.median(),
               
               "starvation_log10_median": usage_df.starved_mem_log10.median(),
               "starvation_log10_mean_cycle_median" : usage_df.groupby("time_ms").starved_mem_log10.mean().median(),
               
               "tasks_run" : len(task_df),
               "tasks_run_preemption" : len(preemption_task_df),
               "tasks_run_success" : len(success_task_df),
               "total_sim_time" : task_df.end_time_ms.max() - task_df.start_time_ms.min()
               }]
    return pandas.DataFrame(values)
