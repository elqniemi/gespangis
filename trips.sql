create or replace function calculate_distance(geom1 geometry, geom2 geometry) returns double precision as $$
begin
    return st_distance(geom1, geom2);
end;
$$ language plpgsql;

create table scooters.trips (
    trip_id serial primary key,
    car_id integer,
    vehicletype_id integer,
    isdamaged boolean,
    avg_min_fuellevel numeric,
    avg_max_fuellevel numeric,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    price_cent integer,
    geom_start geometry,
    geom_end geometry
);


-- process data and create trips
insert into scooters.trips (
    car_id,
    vehicletype_id,
    isdamaged,
    avg_min_fuellevel,
    avg_max_fuellevel,
    start_time,
    end_time,
    price_cent,
    geom_start,
    geom_end
)
with ordered_data as (
    select *,
           lead(geom, 1) over (partition by car_id order by end_time) as next_geom,
           lead(start_time, 1) over (partition by car_id order by end_time) as next_start_time
    from scooters.scooters
),
trip_markers as (
    select *,
           case 
               when calculate_distance(geom::geometry, coalesce(next_geom, geom)::geometry) > 100 
               then 1 
               else 0 
           end as is_trip
    from ordered_data
),
trip_boundaries as (
    select *,
           sum(is_trip) over (partition by car_id order by end_time) as trip_id
    from trip_markers
)
select 
    min(car_id) as car_id,
    min(vehicletype_id) as vehicletype_id,
    bool_or(isdamaged) as isdamaged,
    avg(min_fuellevel) as avg_min_fuellevel,
    avg(max_fuellevel) as avg_max_fuellevel,
    max(end_time) as start_time,
    min(next_start_time) as end_time,
    sum(price_cent) as price_cent,
    min(geom) as geom_start,
    max(next_geom) as geom_end
from trip_boundaries
where is_trip = 1
group by trip_id, car_id
order by car_id, start_time;


