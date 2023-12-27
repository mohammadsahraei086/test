import pandas as pd
from sqlalchemy import create_engine
import finpy_tse as fpy
import psycopg2 as pg
from psycopg2 import sql
import matplotlib.pyplot as plt

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

df.to_sql("market_watch", con=engin, if_exists="replace")

conn = None
try:
    conn = pg.connect(database="stocks_info", user="postgres", password="1234", host="localhost")
except Exception as e:
    print(e)

cursor = conn.cursor()

cursor.execute("""DELETE FROM market_watch 
                  WHERE "trade type" IN ('بلوکی', 'عمده')
                  OR market IN ('صندوق قابل معامله', 'حق تقدم بورس' ,'حق تقدم فرابورس', 'حق تقدم پایه');
                  """)

cursor.execute("""SELECT ticker FROM market_watch
                WHERE (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0) >= 1.5
                AND ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000 > 15
                AND ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000 > 10
                AND low < day_ul
                AND close != day_ul
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
                            AND i_sell_per_capita > 5;""").format(sql.Identifier(stock.strip() + '-روزانه')))

    output = cursor.fetchall()[0]
    if output[0] > 3:
        selected_stocks.append(stock)

print(selected_stocks)

for stock in selected_stocks:
    cursor.execute(sql.SQL("""WITH temp1 AS
                                (SELECT timestamp::date, max(i_buy_pow) as High, min(i_buy_pow) as Low
                                FROM {0} 
                                GROUP BY timestamp::date ORDER BY timestamp::date DESC LIMIT 60)

                            ,temp2 AS
                                (WITH temp_first AS 
                                    (SELECT timestamp::date, i_buy_pow
                                    FROM {0}
                                    WHERE i_buy_pow IS NOT NULL)
                                SELECT DISTINCT timestamp::date, first_value(i_buy_pow)
                                over(PARTITION BY timestamp::date) AS Open
                                FROM temp_first ORDER BY timestamp::date DESC LIMIT 60)

                            ,temp3 AS
                                (WITH temp_last AS 
                                    (SELECT timestamp::date, i_buy_pow
                                    FROM {0}
                                    WHERE i_buy_pow IS NOT NULL)
                                SELECT DISTINCT timestamp::date, last_value(i_buy_pow)
                                over(PARTITION BY timestamp::date) AS Close
                                FROM temp_last ORDER BY timestamp::date DESC LIMIT 60)

                            SELECT temp1.timestamp::date, temp1.High, temp1.Low, temp2.Open, temp3.Close,
                            {1}.volume
                            AS Volume FROM temp1
                            INNER JOIN temp2 on temp1.timestamp::date=temp2.timestamp::date
                            INNER JOIN temp3 on temp3.timestamp::date=temp2.timestamp::date
                            INNER JOIN {1} on {1}.date=temp3.timestamp::date
                            ;""").format(sql.Identifier(stock.strip() + '-سرانه'),
                                         sql.Identifier(stock.strip() + '-روزانه')))

    output1 = cursor.fetchall()

    cursor.execute(sql.SQL("""SELECT date, open, high, low, last, volume FROM {}
                            ORDER BY date DESC LIMIT 59
                            ;""").format(sql.Identifier(stock.strip() + '-روزانه')))

    output2 = cursor.fetchall()

    dataframe1 = pd.DataFrame(output1, columns=['date', 'High', 'Low', 'Open', 'Close', 'Volume'])
    dataframe2 = pd.DataFrame(output2, columns=['date', 'Open', 'High', 'Low', 'Close', 'Volume'])
    #dataframe1['date'] = pd.to_datetime(dataframe1['date'], errors='coerce')
    #dataframe2['date'] = pd.to_datetime(dataframe2['date'], errors='coerce')
    print(dataframe2.date.tolist())

    plt.figure()
    up = dataframe2[dataframe2.Close >= dataframe2.Open]
    down = dataframe2[dataframe2.Close < dataframe2.Open]
    col1 = 'blue'
    col2 = 'green'
    width = .3
    width2 = .03
    plt.bar(up.index, up.Close - up.Open, width, bottom=up.Open, color=col1)
    plt.bar(up.index, up.High - up.Close, width2, bottom=up.Close, color=col1)
    plt.bar(up.index, up.Low - up.Open, width2, bottom=up.Open, color=col1)
    plt.bar(down.index, down.Close - down.Open, width, bottom=down.Open, color=col2)
    plt.bar(down.index, down.High - down.Open, width2, bottom=down.Open, color=col2)
    plt.bar(down.index, down.Low - down.Close, width2, bottom=down.Close, color=col2)
    plt.xticks(dataframe2.index.tolist(), rotation=30, ha='right')
    plt.show()

cursor.close()
conn.commit()
conn.close()
