--DROP TABLE IF EXISTS public.travel_by_taxi_info;
CREATE TABLE IF NOT EXISTS public.travel_by_taxi_info
(
    "VendorID" integer,
    sum_amount double precision,
    count_orders bigint,
    sum_passengers bigint,
    avg_trip_distance double precision,
    min_trip_distance double precision,
    max_trip_distance double precision,
	time_key timestamptz default now()
);