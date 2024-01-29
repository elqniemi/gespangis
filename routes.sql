alter table scooters.trips add column start_vid int;
alter table scooters.trips add column end_vid int;

update scooters.trips
set start_vid = odm.source
from analysis.odm odm
where scooters.trips.trip_id = odm.trip_id;

create table analysis.route_result as
with dijkstra as (
select
	*
from
	pgr_dijkstra(
		'select gid as id, source, target, length_m as cost from network.ways where tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)',
		'select source, target from analysis.odm',
		directed:=false
		)
),
routes as (
	select 
		d.start_vid,
		d.end_vid,
		max(d.agg_cost) as cost_m,
		st_union(w.the_geom) as geom
	from
		dijkstra d
		inner join
		network.ways w
		on
		d.edge = w.gid
	group by
		d.start_vid,
		d.end_vid
)
select
	a.start_vid,
	a.end_vid,
	b.trip_id,
	b.start_time,
	b.end_time,
	b.price_cent,
	b.isdamaged,
	b.avg_max_fuellevel,
	b.avg_min_fuellevel,
	a.cost_m,
	cost_m / 1000 / extract(epoch from b.end_time - b.start_time) / 3600 as avg_speed,
	st_transform(a.geom, 4326) geom
from
	routes a
	inner join
	scooters.trips b
	on
	a.start_vid = b.start_vid and a.end_vid = b.end_vid
;

alter table analysis.route_result add column id serial;

alter table analysis.route_result add column duration interval;

update analysis.route_result
set
duration = end_time - start_time;

