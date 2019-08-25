import sched, time

scheduler = sched.scheduler(time.time, time.sleep)