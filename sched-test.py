import sched, time
import datetime as dt

EST = dt.timezone(-dt.timedelta(hours=4), name='EST')
scheduler = sched.scheduler(time.time, time.sleep)


def trainingfunct(message):
    print ('I am event with a message:', message)


def createRunSchedule():
    '''
    Now run the following schedule on Sunday till 12 pm every 15 min, then wait till Monday 10 AM and 
    contunue till 5PM. Repeat for all other weekdays.
    '''

    run_period_sec = dt.timedelta(seconds = 15*60)
    min_time = dt.time(9,45);max_time = dt.time(16,50)
    min_weekday = 1; max_weekday = 5

    conditions_met = False

    current_datetime =  dt.datetime.now(EST)
    
    print ('Running at:', current_datetime.strftime('%m-%d-%Y %H:%M:%S'))
    time.sleep(1)
    print ('Calculating next running time:')

    while not conditions_met:
        newrun_datetime = current_datetime + run_period_sec
        new_time = newrun_datetime.time()
        new_day = newrun_datetime.isoweekday()
        conditions_met = (new_time>=min_time and new_time<= max_time and
                          new_day >= min_weekday and new_day <= max_weekday)
        print (conditions_met , current_datetime)
        current_datetime = newrun_datetime

createRunSchedule()
