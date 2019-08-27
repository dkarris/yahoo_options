import datetime as dt
import time
from config import EST
import logging

def createRunSchedule():
    '''
    Now run the following schedule on Sunday till 12 pm every 15 min, then wait till Monday 10 AM and 
    contunue till 5PM. Repeat for all other weekdays.
    '''

    run_period_sec = dt.timedelta(seconds = 15)
    min_time = dt.time(9,45);max_time = dt.time(23,50)
    min_weekday = 1; max_weekday = 5

    conditions_met = False

    current_datetime =  dt.datetime.now(EST)
    
    logging.info ('Running at: {}'.format(current_datetime.strftime('%m-%d-%Y %H:%M:%S')))
    logging.info ('Calculating next running time:')

    while not conditions_met:
        newrun_datetime = current_datetime + run_period_sec
        new_time = newrun_datetime.time()
        new_day = newrun_datetime.isoweekday()
        conditions_met = (new_time>=min_time and new_time<= max_time and
                          new_day >= min_weekday and new_day <= max_weekday)
        current_datetime = newrun_datetime

    logging.info ('Next scheduled run: {}'.format(newrun_datetime))
    return dt.datetime.timestamp(newrun_datetime)
