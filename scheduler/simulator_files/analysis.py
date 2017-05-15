import pandas
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.cm as cmx
import matplotlib.colors

def prepare_df(out_trace_file):
    """Reads an output trace file into a dataframe and translates the time columns 
    so the minimum time occurring is 0
    
    Parameters:
    -----------
    out_trace_file : str
        Path to the output trace file from a simulation run
        
    Returns:
    -------
    df : pandas.DataFrame
        dataframe containing the fields from the output trace file"""
    df = pandas.read_csv(out_trace_file)
    min_time = min(df["submit_time_ms"])
    df["start_time_ms"] =df["start_time_ms"]-min_time
    df["end_time_ms"] =df["end_time_ms"]-min_time
    df["submit_time_ms"] = df["submit_time_ms"]-min_time
    df["mesos_start_time_ms"] = df["mesos_start_time_ms"] - min_time
    df["run_time_ms"] = df.end_time_ms - df.start_time_ms
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
    df["overhead"] = df.submit_to_complete_ms - df.run_time_ms
    df.reset_index(inplace=True)
    return df

def compare_job_traces(run_x, run_y):
    """Given two dfs, run_x and run_y from job_view_stats,
    returns a single dataframe where each row is comparing
    the same job. Adds column 'ratio' which informs how many times better
    run_y was compared to run_x for that job. For example, if job A had an 
    overhead of 5 seconds in run_y and an overhead of 10 seconds in run_x, than
    ratio for job A would be 2, i.e. job A suffered 2x less overhead in run_y 
    than in run_x
    
    Parameters:
    -----------
    run_x : pandas.DataFrame
        job_view_stats outputted dataframe
    run_y : pandas.DataFrame
        job_view_stats outputted dataframe
        
    Returns:
    --------
    df : pandas.DataFrame
        joined dataframe of run_x and run_y with ratio column"""
    compare_df = run_x.merge(run_y, on="job_id")
    compare_df["ratio"] = compare_df.overhead_x/compare_df.overhead_y
    return compare_df

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
    
def analyze_state(df, t):
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
    state_df = running_tasks_at(df, t)
    state_df["count"] = 1
    per_host = state_df.groupby("hostname").sum()[["mem", "cpus", "count"]]
    per_user = state_df.groupby("user").sum()[["mem", "cpus", "count"]]
    waiting = df[(df.submit_time_ms < t) & (df.start_time_ms > t)]
    return [per_host, per_user, waiting, state_df, df]


def gantt_plot(names, starts, ends, colors=None, groups=None, texts=None):
    if colors is None:
        if groups is None:
            c_map = plt.get_cmap("jet")
            cNorm  = matplotlib.colors.Normalize(vmin=0, vmax=len(starts))
            scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=c_map)
            colors = [scalarMap.to_rgba(i) for i in range(len(starts))]
    names = names.values
    unique_names, index =  np.unique(names, return_index=True)
    unique_names = list(names[sorted(index)])
    #This is O(nm) where n is names and m is unique names. Can optimize
    plt.barh(bottom=[unique_names.index(name) for name in names], left=starts, width=ends-starts, color=colors)
    if texts is not None:
        for i, text in enumerate(texts):
            plt.text(np.mean([starts[i]]), unique_names.index(names[i]), text, 
                       verticalalignment='center', fontsize=30)
    
def colors_for_values(vs):
    unique_vs = list(np.unique(vs))
    c_map = plt.get_cmap("jet")
    cNorm  = matplotlib.colors.Normalize(vmin=0, vmax=len(unique_vs))
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=c_map)
    return [scalarMap.to_rgba(unique_vs.index(v)) for v in vs]

def time_series_events(events):
    """Given a list of events, [time_ms, count, cpus, mem], 
    returns a dataframe where each row is the utilization at a given time
     
     Parameters:
     -----------
     events : list of tuples [time_ms, count, cpus, mem]
         time series of events, such as job starting or completing
         
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
    """Given a dataframe of tasks, returns a dataframe where each row 
    is the resources waiting at a given time
    
     
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
         1. 'count' is the number of tasks waiting 
         2. 'mem' is the memory waiting 
         3. 'cpus' is the cpus waiting 
         at time 'time_ms'."""
    rows = df.to_records()
    events = [e for r in rows for e in [(r["submit_time_ms"], 1, r["mem"], r["cpus"]), 
                                        (r["start_time_ms"], -1, -r["mem"], -r["cpus"])]]
    return time_series_events(events)

    
