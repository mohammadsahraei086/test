from sqlalchemy import create_engine
import finpy_tse as fpy
import psycopg2 as pg
from psycopg2 import sql

engin = create_engine("postgresql+psycopg2://postgres:1234@localhost:5432/stocks_info")

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
df.index.name = "ticker"

df.to_sql("market_watch_data", con=engin, if_exists="replace")

conn = None
try:
    conn = pg.connect(database="stocks_info", user="postgres", password="1234", host="localhost")
except Exception as e:
    print(e)

cursor = conn.cursor()
cursor.execute("""DELETE FROM market_watch_data 
                  WHERE "trade type" IN ('بلوکی', 'عمده')
                  OR market IN ('صندوق قابل معامله', 'حق تقدم بورس' ,'حق تقدم فرابورس', 'حق تقدم پایه');
                  """)

cursor.execute("""SELECT ticker FROM market_watch_data
                WHERE (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0) >= 1.5
                AND ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000 > 15
                AND ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000 > 10
                AND low < day_ul
                AND close = day_ul
                ORDER BY (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0)""")

filtered_stocks = [stock[0] for stock in cursor.fetchall()]
print(filtered_stocks)
selected_stocks = []
for stock in filtered_stocks:
    cursor.execute(sql.SQL("""WITH temp_table AS
                            (SELECT * FROM {0} ORDER BY date DESC LIMIT 5)
                            SELECT count(date)
                            FROM temp_table WHERE i_buy_pow >= 1 
                            AND low < up_level 
                            AND i_buy_per_capita > 15
                            AND i_sell_per_capita > 5;""").format(sql.Identifier(stock.strip()+'-روزانه')))

    output = cursor.fetchall()[0]
    if output[0] > 3:
        selected_stocks.append(stock)

print(selected_stocks)

cursor.close()
conn.commit()
conn.close()
