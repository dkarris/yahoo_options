import requests
import datetime as dt
import sqlite3
import logging


from config import EST, scheduler
from schedule_func import createRunSchedule

#EST = dt.timezone(-dt.timedelta(hours=4), name='EST') # 4 hours from UTC (run on Colab) - to put EST zone datetime stamp

# setup sqlite connection

logging.basicConfig(filename='log.txt', level = logging.DEBUG,
                    format = '%(asctime)s %(message)s', datefmt = '%m-%d-%Y %H:%M:%S')



conn = sqlite3.connect('options_v2.db')
c = conn.cursor()

# create table if doesn't exist
c.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name = 'options' ''')
if c.fetchone()[0] == 0:
    c.execute('''CREATE TABLE options 
                (n INTEGER PRIMARY KEY,timestamp text, fullname text, asset text, assetprice real,
                    optiontype text, strike real, 
                    bid real, ask real, lastprice real, openinterest real,
                    impliedvola real, inthemoney text, expiration text)
                ''')



def getOptionsChainYahoo(ticker,depth):
    '''
    '''
    data_url = "https://query2.finance.yahoo.com/v7/finance/options/{}".format(ticker)
    response = requests.get(data_url).json()
    if  len(response['optionChain']['result']) != 0:
        sqlrecords = []
        fields = ['contractSymbol','strike','lastPrice','bid','ask',
                  'expiration','impliedVolatility','inTheMoney', 'openInterest']

        expiryDates = response['optionChain']['result'][0]['expirationDates'][0:depth]
        
        for expiryDate in expiryDates:
            options = []
            data_url = "https://query2.finance.yahoo.com/v7/finance/options/{}?date={}".format(ticker, expiryDate)
            response = requests.get(data_url).json()
            calls = response['optionChain']['result'][0]['options'][0]['calls']
            puts = response['optionChain']['result'][0]['options'][0]['puts']

            assetprice = response['optionChain']['result'][0]['quote']['regularMarketPrice']
            request_datetime = response['optionChain']['result'][0]['quote']['regularMarketTime']
            request_datetime = dt.datetime.fromtimestamp(
                request_datetime, EST).strftime('%m-%d-%Y %H:%M:%S')

            for call in calls:
                call['optiontype'] = 'call'
                options.append(call)
            for put in puts:
                put['optiontype'] = 'put'
                options.append(put)

            for option in options:
                for field in fields:
                    if field not in option.keys():
                        option[field] = 0
                _od = dict(option) #_od = opt dict
                expiry = dt.datetime.fromtimestamp(
                    option['expiration']).strftime('%m-%d-%Y 16:00:00')
                record = (request_datetime, _od['contractSymbol'], ticker, assetprice,
                        _od['optiontype'],_od['strike'], _od['bid'], _od['ask'],
                        _od['lastPrice'], _od['openInterest'],
	                _od['impliedVolatility'], _od['inTheMoney'], expiry)
                sqlrecords.append(record)

        # save into sqlite
        try:
            c.executemany(
                '''INSERT INTO options (timestamp, fullname, asset, assetprice ,
                    optiontype, strike, bid, ask, lastprice , openinterest,
                    impliedvola, inthemoney, expiration) VALUES (
                        ?,?,?,?,?,?,?,?,?,?,?,?,?)''', sqlrecords)
            conn.commit()
            return True
        except:
            return None
    else:
        return None

def runAPIquery(tickerlist):
    for ticker in tickerlist:
        result = getOptionsChainYahoo(ticker,4)
        logging.info('Saved data for ticker {}'.format(ticker))
        if  not result: 
            logging.info('API querying failed for ticker {}'.format(ticker))
            

def mainloop():
    tickerlist = ['USO', 'QQQ', 'SLV','SMH', 'JNJ']
    runAPIquery(tickerlist)
    logging.info ('****!!!!!!!! Launching new cycle from inner loop **************!')
    scheduler.enterabs(createRunSchedule(),1,mainloop)

launch_time = createRunSchedule()
scheduler.enterabs(launch_time,1,mainloop)
logging.info('*******************Launching cycle from the main loop*****************')
scheduler.run()
