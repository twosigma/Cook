import logging
import pandas
import pandas.util.testing as pdt
import numpy as np
import unittest

import analysis as a


class AnalysisTest(unittest.TestCase):
    def setUp(self):
        pass

    def test_prepare_df(self):
        df = pandas.DataFrame([(-10, 1, 2, 10),
                               (0, 1, 4, 12),
                               (0, 1, 5, 6),
                               (100, 101, 102, 130),
                               (100, 101, 102, 90)],
                              columns=['submit_time_ms', 'start_time_ms', 
                                       'mesos_start_time_ms', 'end_time_ms'])
        expected_df = pandas.DataFrame([(0,   11,  12,  20,  9,  11),
                                        (10,  11,  14,  22,  11, 1),
                                        (10,  11,  15,  16,  5,  1),
                                        (110, 111, 112, 140, 29, 1),
                                        (110, 111, 112, 140, 29, 1)],
                                       columns=[
                                           'submit_time_ms', 'start_time_ms', 
                                           'mesos_start_time_ms', 'end_time_ms',
                                           'run_time_ms', 'overhead'
                                       ])
        pdt.assert_frame_equal(expected_df, a.prepare_df(df))

    def test_running_tasks_at(self):
        df = pandas.DataFrame([(0,   11,  12,  20,  9,   11),
                               (10,  11,  14,  22,  11,  1),
                               (10,  11,  15,  140, 129, 1),
                               (110, 111, 112, 140, 29,  1),
                               (110, 111, 112, 140, 29,  1)],
                              columns=[
                                  'submit_time_ms', 'start_time_ms', 
                                  'mesos_start_time_ms', 'end_time_ms',
                                  'run_time_ms', 'overhead'
                              ])

        expected_df = pandas.DataFrame([(0,   11,  12,  20,  9,   11),
                                        (10,  11,  14,  22,  11,  1),
                                        (10,  11,  15,  140, 129, 1),
                                        ],
                                       columns=[
                                           'submit_time_ms', 'start_time_ms', 
                                           'mesos_start_time_ms', 'end_time_ms',
                                           'run_time_ms', 'overhead'
                                       ])
        # don't care about index
        actual_df = a.running_tasks_at(df, 12).reset_index(drop=True)

        pdt.assert_frame_equal(expected_df, actual_df)

        expected_df = pandas.DataFrame([(10,  11,  15,  140, 129, 1),
                                        (110, 111, 112, 140, 29,  1),
                                        (110, 111, 112, 140, 29,  1)],
                                       columns=[
                                           'submit_time_ms', 'start_time_ms', 
                                           'mesos_start_time_ms', 'end_time_ms',
                                           'run_time_ms', 'overhead'
                                       ])

        # don't care about index
        actual_df = a.running_tasks_at(df, 112).reset_index(drop=True)

        pdt.assert_frame_equal(expected_df, actual_df) 


    def test_time_series_events(self):
        # tuple is (time, count, mem, cpus)
        events = [(0, 1, 1, 1),
                  (1, -1, -1, -1),
                  (2, 1, 10, 10),
                  (3, 1, 20, 20),
                  (4, 1, 30, 20),
                  (5, -1, -50, -50),
                  (3, 1, 50, 50)
                  ]
        rows = [{"time_ms" : 0, "count" : 1, "mem" : 1, "cpus" : 1},
                {"time_ms" : 1, "count" : 0, "mem" : 0, "cpus" : 0},
                {"time_ms" : 2, "count" : 1, "mem" : 10, "cpus" : 10},
                {"time_ms" : 3, "count" : 2, "mem" : 30, "cpus" : 30},
                {"time_ms" : 3, "count" : 3, "mem" : 80, "cpus" : 80},
                {"time_ms" : 4, "count" : 4, "mem" : 110, "cpus" : 100},
                {"time_ms" : 5, "count" : 3, "mem" : 60, "cpus" : 50}
                ]
        expected_df = pandas.DataFrame(rows)
        events_df = a.time_series_events(events)
        pdt.assert_frame_equal(events_df, expected_df)

    def test_get_fair_allocation_one_user(self):
        usage_rows = [(0, 'a', 0, 1838.0)]
        usage_columns = ['time_ms', 'user','mem','mem_running']
        usage_df = pandas.DataFrame(usage_rows,
                                    columns=usage_columns)
        fair_df = a.get_fair_allocation(usage_df)
        fair_df = fair_df.sort_values('user').reset_index(drop=True)

        expected_df = pandas.DataFrame([('a', 1838.0, 0)],
                                       columns=['user', 'mem', 'time_ms'])
        pdt.assert_frame_equal(expected_df, fair_df)



    def test_get_fair_allocation_simple(self):
        usage_rows = [(0, 'a', 100, 250),
                      (0, 'b', 0, 250),
                      (0, 'c', 100, 250),
                      (0, 'd', 1000, 250)]
        usage_columns = ['time_ms', 'user','mem','mem_running']
        usage_df = pandas.DataFrame(usage_rows,
                                    columns=usage_columns)
        fair_df = a.get_fair_allocation(usage_df)
        fair_df = fair_df.sort_values('user').reset_index(drop=True)

        expected_df = pandas.DataFrame([('a', 250, 0), 
                                        ('b', 250, 0),
                                        ('c', 250, 0),
                                        ('d', 250, 0)],
                                       columns=['user', 'mem', 'time_ms'])
        pdt.assert_frame_equal(expected_df, fair_df)

            
    def test_get_fair_allocation_complex(self):
       usage_rows = [(0, 'a', 100, 250),
                     (0, 'b', 0, 250),
                     (0, 'c', 1000, 250),
                     (0, 'd', 1000, 1250)]
       usage_columns = ['time_ms', 'user','mem','mem_running']
       usage_df = pandas.DataFrame(usage_rows,
                                   columns=usage_columns)
       fair_df = a.get_fair_allocation(usage_df)
       fair_df = fair_df.sort_values('user').reset_index(drop=True)
    
       expected_df = pandas.DataFrame([('a', 350.0, 0), 
                                       ('b', 250.0, 0),
                                       ('c', 700.0, 0),
                                       ('d', 700.0, 0)],
                                      columns=['user', 'mem', 'time_ms'])
       pdt.assert_frame_equal(expected_df, fair_df)

    
    def test_sample_usage(self):
        user_event_columns = ['time_ms', 'user', 'mem', 'cpus', 'count']
        user_running = pandas.DataFrame([(0, 'a', 10, 1, 1),
                                         (0, 'b', 12, 2, 2),
                                         (1, 'a', 40, 20, 20),
                                         (2, 'a', 20, 2, 2),
                                         (3, 'b', 9, 1, 1),
                                         (5, 'c', 30, 3, 3),
                                         (6, 'c', 30, 3, 3)],
                                        columns=user_event_columns)
        user_waiting = pandas.DataFrame([(0, 'a', 10, 1, 1),
                                         (0, 'b', 12, 2, 2),
                                         (2, 'a', 20, 2, 2),
                                         (3, 'b', 90, 10, 10),
                                         (5, 'c', 30, 3, 3)],
                                        columns=user_event_columns)
        usage_columns = ['time_ms', 'user', 'mem', 'cpus', 'count', 
                         'mem_running', 'cpus_running', 'count_running']
        nan = np.NaN
        expected_df = pandas.DataFrame([(0, 'a', 10, 1, 1, 10, 1, 1),
                                        (0, 'b', 12, 2, 2, 12, 2, 2),
                                        (0, 'c', nan, nan, nan, nan, nan, nan),
                                        (5, 'a', 20, 2, 2, 20, 2, 2),
                                        (5, 'b', 90, 10, 10, 9, 1, 1),
                                        (5, 'c', 30, 3, 3, 30, 3, 3)],
                                       columns=usage_columns)
        actual_df = a.sample_usage(user_running, user_waiting, 5)
        actual_df = actual_df.reset_index(drop=True)
        pdt.assert_frame_equal(expected_df, actual_df)
