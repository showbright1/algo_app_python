import sqlite3

def get_db():
    global algoDB
    algoDB = sqlite3.connect('algo.db')
    create_table()
    return algoDB

def create_table():
    algoDB.execute("""
    CREATE TABLE IF NOT EXISTS instruments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instrument TEXT NOT NULL,
        instrument_token INTEGER NOT NULL,
        last_price INTEGER NOT NULL,
        open INTEGER NOT NULL,
        high INTEGER NOT NULL,
        low INTEGER NOT NULL,
        close INTEGER NOT NULL,
        volume INTEGER NOT NULL,
        exchange_timestamp datetime,
        created TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        created_local DATETIME DEFAULT (DATETIME(CURRENT_TIMESTAMP, 'LOCALTIME'))
    )
    """
    )
    
    # algoDB.execute("INSERT INTO instruments (username, password) VALUES ('admin', 'admin123')")
    
def insert_instrument(data):
    q = f"INSERT INTO instruments (instrument, instrument_token, last_price, open, high, low, close, volume, exchange_timestamp) VALUES ('{data['instrument']}', {data['instrument_token']}, {data['last_price']}, {data['open']}, {data['high']}, {data['low']}, {data['close']}, {data['volume']}, {data['exchange_timestamp']})"
    print(q)
    algoDB.execute(q)
    algoDB.commit()
    
def get_instruments():
    data = algoDB.execute('SELECT * FROM instruments')
    # algoDB.commit()
    return data

def create_tables(tokens):
    c = algoDB.cursor()
    for t in tokens:
        c.execute("""CREATE TABLE IF NOT EXISTS TOKEN{} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            ts datetime,
            price real(15,5), 
            open INTEGER NOT NULL,
            high INTEGER NOT NULL,
            low INTEGER NOT NULL,
            close INTEGER NOT NULL,
            volume integer
            )""".format(t))
        
    try:
        algoDB.commit()
    except:
        algoDB.rollback()
    
def insert_ticks(ticks):
    c=algoDB.cursor()
    for tick in ticks:
        #try:
        t_name = "TOKEN"+str(tick['instrument_token'])
        vals = [tick['exchange_timestamp'],tick['last_price'], tick['ohlc']['open'], tick['ohlc']['high'], tick['ohlc']['low'], tick['ohlc']['close'], tick['last_traded_quantity']]
        query = "INSERT INTO {}(ts,price,open,high,low,close,volume) VALUES (?,?,?,?,?,?,?)".format(t_name)
        c.execute(query,vals)
        #except:
            #pass
    algoDB.commit()
    # try:
    #     algoDB.commit()
    # except:
    #     algoDB.rollback()    
