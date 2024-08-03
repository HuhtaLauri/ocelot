CREATE DATABASE taxi;

\c taxi

CREATE TABLE public.taxi_trips (
    VendorID VARCHAR,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INTEGER,
    trip_distance NUMERIC(10, 2),
    RatecodeID VARCHAR,
    store_and_fwd_flag VARCHAR,
    PULocationID VARCHAR,
    DOLocationID VARCHAR,
    payment_type VARCHAR,
    fare_amount NUMERIC(10, 2),
    extra NUMERIC(10, 2),
    mta_tax NUMERIC(10, 2),
    tip_amount NUMERIC(10, 2),
    tolls_amount NUMERIC(10, 2),
    improvement_surcharge NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    congestion_surcharge NUMERIC(10, 2),
    airport_fee NUMERIC(10, 2),
    PRIMARY KEY (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, PULocationID, DOLocationID, total_amount)
);
