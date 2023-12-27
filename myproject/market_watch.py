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
    # cursor.execute(sql.SQL("""DROP TABLE IF EXISTS {}""").format(sql.Identifier(stock.strip()+'-سرانه')))
    cursor.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {}
                            (timestamp TIMESTAMP UNIQUE,
                            last_price INTEGER,
                            i_buy_pow FLOAT4,
                            i_buy_per_capita INT,
                            i_sell_per_capita INT,
                            i_entered_money INT,
                            i_buy_ratio FLOAT4,
                            i_sell_ratio FLOAT4)
                            """).format(sql.Identifier(stock.strip()+'-سرانه')))

    cursor.execute(sql.SQL("""ALTER TABLE {}
                                ADD COLUMN IF NOT EXISTS last_percent FLOAT4,
                                ADD COLUMN IF NOT EXISTS volume INT8
                                ;""").format(sql.Identifier(stock.strip() + '-سرانه')))

    cursor.execute(sql.SQL("""INSERT INTO {}
                            (timestamp,
                            last_price,
                            i_buy_pow,
                            i_buy_per_capita,
                            i_sell_per_capita,
                            i_entered_money,
                            i_buy_ratio,
                            i_sell_ratio,
                            last_percent,
                            volume)
                            SELECT 
                            download::TIMESTAMP,
                            close::INT,
                            (vol_buy_r/NULLIF(no_buy_r,0))/NULLIF((vol_sell_r/NULLIF(no_sell_r,0)),0),
                            ((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000, 
                            ((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000,
                            ((vol_sell_i - vol_buy_i)*final)/10000000,
                            vol_buy_r/NULLIF(volume,0),
                            vol_sell_r/NULLIF(volume,0),
                            close_percent::FLOAT4,
                            volume::INT8
                            FROM market_watch
                            WHERE
                            ticker=(%s)""").format(sql.Identifier(stock.strip()+'-سرانه')), (stock,))

cursor.execute("""DELETE FROM market_watch 
                WHERE "ticker" IN ('آسیاتک', 'پی پاد', 'ددانا', 'سآبیک', 'فرود', 'گلدیر', 'درازی'
                , 'فروژ', 'وطوبی', 'اردستان', 'انتخاب', 'وکغدیر', 'فگستر' ,'فزر' , 'فتوسا', 'غکورش', 'ولکار'
                , 'ومدیر', 'لطیف', 'سپید', 'وهامون', 'مدیریت', 'کیمیاتک', 'فجهان', 'فسبزوار', 'توسن'
                , 'صبا', 'غگیلا', 'پیزد', 'آریا', 'وپویا', 'سیتا', 'ولپارس', 'رافزا', 'آبادا', 'بگیلان'
                , 'غزر', 'شاروم', 'امین', 'زملارد', 'گدنا', 'ثبهساز', 'بپیوند', 'ثامید'
                , 'وسپهر', 'چخزر', 'بوعلی', 'وکبهمن', 'سپیدار'
                , 'کزغال', 'اپال', 'ساوه', 'زکوثر', 'شصدف', 'کصدف')
                ;""")

cursor.execute("""CREATE TABLE IF NOT EXISTS market_sarane
                        (timestamp TIMESTAMP UNIQUE,
                        i_buy_pow FLOAT4,
                        i_buy_per_capita INT,
                        i_sell_per_capita INT,
                        i_entered_money INT)
                        ;""")

cursor.execute("""INSERT INTO market_sarane
                        (timestamp,
                        i_buy_pow,
                        i_buy_per_capita,
                        i_sell_per_capita,
                        i_entered_money)
                        SELECT 
                        DISTINCT download::TIMESTAMP,
                        avg(((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000)/NULLIF(
                            avg(((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000),0),
                        avg(((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000), 
                        avg(((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000),
                        sum(((vol_sell_i - vol_buy_i)*final)/10000000)
                        FROM market_watch
                        GROUP BY download
                        ;""")

cursor.execute("""CREATE TABLE IF NOT EXISTS large_sarane
                        (timestamp TIMESTAMP UNIQUE,
                        i_buy_pow FLOAT4,
                        i_buy_per_capita INT,
                        i_sell_per_capita INT,
                        i_entered_money INT)
                        ;""")

cursor.execute("""INSERT INTO large_sarane
                        (timestamp,
                        i_buy_pow,
                        i_buy_per_capita,
                        i_sell_per_capita,
                        i_entered_money)
                        SELECT 
                        DISTINCT download::TIMESTAMP,
                        avg(((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000)/NULLIF(
                            avg(((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000),0),
                        avg(((vol_buy_r*final)/NULLIF(no_buy_r,0))/10000000), 
                        avg(((vol_sell_r*final)/NULLIF(no_sell_r,0))/10000000),
                        sum(((vol_sell_i - vol_buy_i)*final)/10000000)
                        FROM market_watch
                        WHERE "ticker" in ('فارس', 'فولاد', 'فملی', 'کگل', 'شستا', 'شپنا', 'شبندر',
                        'شتران', 'وبملت', 'خودرو', 'خساپا', 'وتجارت', 'وبصادر')
                        GROUP BY download
                        ;""")


cursor.close()
conn.commit()
conn.close()
