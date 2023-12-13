import json
from io import StringIO
import time
import psycopg2
import sqlite3
import duckdb
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,func, desc,extract
from sqlalchemy.orm import sessionmaker,declarative_base

queries = [lambda cursor: cursor.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;'),
    lambda cursor: cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;'),
    lambda cursor: cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;'),
    lambda cursor: cursor.execute('SELECT passenger_count, extract(year from tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
]

queries_sqlite3 = [lambda cursor: cursor.execute('SELECT "VendorID", count(*) FROM trips GROUP BY 1;'),
    lambda cursor: cursor.execute('SELECT passenger_count, avg(total_amount) FROM trips GROUP BY 1;'),
    lambda cursor: cursor.execute('SELECT passenger_count, strftime("%Y", tpep_pickup_datetime), count(*) FROM trips GROUP BY 1, 2;'),
    lambda cursor: cursor.execute('SELECT passenger_count, strftime("%Y", tpep_pickup_datetime), round(trip_distance), count(*) FROM trips GROUP BY 1, 2, 3 ORDER BY 2, 4 desc;')
]

queries_Pandas=[
    lambda df:df.groupby('VendorID').size(),
    lambda df:df.groupby("passenger_count")["total_amount"].mean(),
    lambda df:df.assign(year=pd.to_datetime(df["tpep_pickup_datetime"]).dt.year).groupby(["passenger_count", "year"]).size(),
    lambda df:df.assign(year=pd.to_datetime(df["tpep_pickup_datetime"]).dt.year, distance=df["trip_distance"].round()).groupby(["passenger_count", "year", "distance"]).size().to_frame('size').reset_index().sort_values(['year','size'],ascending=[True,False])
]


def uploading_to_a_engine(engine,data):
    df = pd.read_csv(data)
    if 'Airport_fee' in df:
        df.pop('Airport_fee')
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    df.rename(columns={'Unnamed: 0':'id'}, inplace=True )
    df.to_sql('trips',engine,if_exists='replace', index=False,chunksize=100000)

def measurement_time(queries,cursor):
    n=10
    result=[]
    for query in queries:
        avg = 0
        for _ in range(n):
            t0 = time.perf_counter()
            query(cursor)
            avg+=time.perf_counter() - t0
        result.append(avg/n)
    return result


def psycopg2_test(conf):
    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=False)
    if conf["psycopg2"]["loading the database"] == "True":
        data = 'data/nyc_yellow_big.csv'
        uploading_to_a_engine(engine, data)
       
    conn = psycopg2.connect(user="postgres", password="postgres", host="localhost", port="5432")
    cursor = conn.cursor()
    result=measurement_time(queries,cursor)
    cursor.close()
    conn.close()
    return result


def SQLite_test(conf):
    conn = sqlite3.connect('C:/Users/User/PycharmProjects/lab3/data/data_sqlite.db') 
    cursor = conn.cursor()
    if conf["SQLite"]["loading the database"] == "True":
        data = 'data/nyc_yellow_big.csv'
        uploading_to_a_engine(conn, data)
    result=measurement_time(queries_sqlite3,cursor)
    cursor.close()
    conn.close()
    return result
    

def DuckDB_test(conf):
    conn = duckdb.connect('C:/Users/User/PycharmProjects/lab3/data/data_DuckDB.db') 
    cursor = conn.cursor()
    if conf["DuckDB"]["loading the database"] == "True":
        data = "'data/nyc_yellow_big.csv'"
        cursor.execute(f"CREATE TABLE IF NOT EXISTS trips AS SELECT * FROM read_csv_auto({data});")
    result=measurement_time(queries, cursor)
    cursor.close()
    conn.close()
    return result
    

def SQLAlchemy_test(conf):
    base = declarative_base()
    class trips(base):
        __tablename__ = 'trips'

        id = Column(Integer, primary_key=True)
        Airport_fee = Column(Float)
        VendorID = Column(Integer)
        congestion_surcharge = Column(Float)
        airport_fee = Column(Float)
        passenger_count = Column(Float)
        trip_distance = Column(Float)
        RatecodeID = Column(Float)
        PULocationID = Column(Integer)
        DOLocationID = Column(Integer)
        payment_type = Column(Integer)
        fare_amount = Column(Float)
        extra = Column(Float)
        mta_tax = Column(Float)
        tip_amount = Column(Float)
        tolls_amount = Column(Float)
        improvement_surcharge = Column(Float)
        total_amount = Column(Float)
        tpep_pickup_datetime = Column(DateTime)
        tpep_dropoff_datetime = Column(DateTime)
        store_and_fwd_flag = Column(String)

    queries_SQLAlchemy = [
        lambda session: session.query(trips.VendorID, func.count().label('count')).group_by(trips.VendorID).all(),
        lambda session: session.query(trips.passenger_count, func.avg(trips.total_amount)).group_by(trips.passenger_count).all(),
        lambda session: session.query(trips.passenger_count, extract('year', trips.tpep_pickup_datetime),func.count().label('count')).group_by(trips.passenger_count,extract('year', trips.tpep_pickup_datetime)).all(),
        lambda session: session.query(trips.passenger_count, extract('year', trips.tpep_pickup_datetime), func.round(trips.trip_distance),func.count().label('count')).group_by(trips.passenger_count,extract('year', trips.tpep_pickup_datetime),func.round(trips.trip_distance)).order_by(extract('year', trips.tpep_pickup_datetime),desc(func.count().label('count'))).all()
    ]

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=False)
    if conf["SQLAlchemy"]["loading the database"] == "True":
        data = 'data/nyc_yellow_big.csv'
        uploading_to_a_engine(engine, data)
    Session = sessionmaker(bind = engine,autoflush=False,autocommit = False)
    session = Session()
    
    result=measurement_time(queries_SQLAlchemy,session)
    session.close()
    return result

def Pandas_test(conf):
    data = 'data/nyc_yellow_big.csv'
    df = pd.read_csv(data)
    result=measurement_time(queries_Pandas,df)
    return result


libraries = ["psycopg2","SQLite","DuckDB","SQLAlchemy","Pandas"]
funk_lib = [psycopg2_test,SQLite_test,DuckDB_test,SQLAlchemy_test,Pandas_test]

with open('config/conf.json') as f:
    conf = json.load(f)
    
    res=dict()
    for i in range(len(libraries)):
        if conf[libraries[i]]["start"] == "True":
            res[libraries[i]] = funk_lib[i](conf)
    
    print(res)
    df = pd.read_json(StringIO(json.dumps(res)))
    df.to_csv('graphics/res.csv', encoding='utf-8', index=False)
