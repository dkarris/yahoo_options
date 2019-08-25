import requests
import datetime as dt
import sqlite3

EST = dt.timezone(-dt.timedelta(hours=4), name='EST') # 4 hours from UTC (run on Colab) - to put EST zone datetime stamp

# setup sqlite connection


conn = sqlite3.connect('options.db')
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



def getOptionsChainYahoo(ticker):
    '''
    '''
    data_url = "https://query2.finance.yahoo.com/v7/finance/options/{}".format(ticker)
    response = requests.get(data_url).json()
    if  len(response['optionChain']['result']) != 0:
        options = []
        calls = response['optionChain']['result'][0]['options'][0]['calls']
        puts = response['optionChain']['result'][0]['options'][0]['puts']

        for call in calls:
            call['optiontype'] = 'call'
            options.append(call)
        for put in puts:
            put['optiontype'] = 'put'
            options.append(put)

        assetprice = response['optionChain']['result'][0]['quote']['regularMarketPrice']
        request_datetime = response['optionChain']['result'][0]['quote']['regularMarketTime']
        request_datetime = dt.datetime.fromtimestamp(
            request_datetime, EST).strftime('%m-%d-%Y %H:%M:%S')


        fields = ['contractSymbol','strike','lastPrice','bid','ask',
                  'expiration','impliedVolatility','inTheMoney', 'openInterest']
        for option in options:
            for field in fields:
                if field not in option.keys():
                    option[field] = 0
    
    
        sqlrecords = []
        for option in options:
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
    # time.sleep(1)
    for ticker in tickerlist:
        result = getOptionsChainYahoo(ticker)
        if  not result: 
            print ('API querying failed for ticker {}'.format(ticker))

tickerlist = ['USO', 'QQQ', 'SLV','JNJ']
runAPIquery(tickerlist)