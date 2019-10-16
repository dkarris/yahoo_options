import sched, time
import datetime as dt


EST = dt.timezone(-dt.timedelta(hours=4), name='EST')
scheduler = sched.scheduler(time.time, time.sleep)



