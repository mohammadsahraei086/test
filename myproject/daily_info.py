from sqlalchemy import create_engine
import finpy_tse as fpy
import psycopg2 as pg
from psycopg2 import sql

engin = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/stocks_test")

stock_tuple = []
for _ in range(20):
    try:
        stock_tuple = fpy.Get_MarketWatch()
        break
    except Exception as e:
        print(e)
        continue
else:
    print("NOT Succeeded")

df = stock_tuple[0]
df.columns = df.columns.str.lower()
df.columns = df.columns.str.replace("\(%\)", "_percent", regex=True)
df.index.name = "ticker"

df.to_sql("market_watch", con=engin, if_exists="replace")

conn = None
try:
    conn = pg.connect(database="stocks_test", user="postgres", password="1234", host="localhost")
except Exception as e:
    print(e)


cursor = conn.cursor()
cursor.execute("""ALTER TABLE market_watch
                ALTER COLUMN time TYPE TIME
                USING time::time without time zone
                ;""")

cursor.execute("""DELETE FROM market_watch 
                WHERE "trade type" IN ('بلوکی', 'عمده')
                OR market IN ('صندوق قابل معامله', 'حق تقدم بورس' ,'حق تقدم فرابورس', 'حق تقدم پایه')
                OR time < '09:00:00'::TIME
                OR time > '14:00:00'::TIME;
                ;""")

cursor.execute("""SELECT ticker FROM market_watch""")

list_of_market = [stock[0] for stock in cursor.fetchall()]
for stock in list_of_market:
    # cursor.execute(sql.SQL("""DROP TABLE IF EXISTS {}""").format(sql.Identifier(stock.strip()+'-روزانه')))
    cursor.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {}
                            (date DATE UNIQUE,
                            i_buy_pow FLOAT4,
                            i_buy_per_capita INT,
                            i_sell_per_capita INT,
                            last INTEGER,
                            last_percent FLOAT4,
                            close INTEGER,
                            close_percent FLOAT4,
                            volume INT8,
                            average_volume INT8,
                            i_entered_money INT,
                            eps INTEGER,
                            i_buy_ratio FLOAT4,
                            i_sell_ratio FLOAT4,
                            open INTEGER,
                            high INTEGER,
                            low INTEGER,
                            up_level INTEGER,
                            dawn_level INTEGER)
                            """).format(sql.Identifier(stock.strip()+'-روزانه')))

    cursor.execute(sql.SQL("""INSERT INTO {0}
                            (date,
                            i_buy_pow,
                            i_buy_per_capita,
                            i_sell_per_capita,
                            last,
                            last_percent,
                            close,
                            close_percent,
                            volume,
                            average_volume,
                            i_entered_money,
                            eps,
                            i_buy_ratio,
                            i_sell_ratio,
                            open,
                            high,
                            low,
                            up_level,
                            dawn_level)
                            SELECT 
                            download::TIMESTAMP::DATE,
                            (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0),
                            ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000, 
                            ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000,
                            close::INT,
                            close_percent::FLOAT4,
                            final::INT,
                            final_percent::FLOAT4,
                            volume::INT8,
                            (SELECT AVG(volume) OVER(ORDER BY date ROWS BETWEEN 41 PRECEDING AND CURRENT ROW) FROM {0} 
                            ORDER BY date DESC LIMIT 1),
                            ((vol_sell_i - vol_buy_i)*final)/10000000,
                            eps::INT,
                            vol_buy_r/NULLIF(volume,0),
                            vol_sell_r/NULLIF(volume,0),
                            open::INT,
                            high::INT,
                            low::INT,
                            day_ul::INT,
                            day_ll::INT
                            FROM market_watch
                            WHERE
                            ticker=%s;""").format(sql.Identifier(stock.strip()+'-روزانه')), (stock,))

cursor.close()
conn.commit()
conn.close()
