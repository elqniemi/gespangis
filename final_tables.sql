create or replace function analyze_routes()
returns setof record as $$
declare
    route record;
    cell record;
    clipped_geom geometry;
    merged_geom geometry;
    individual_geom geometry;
    segment_bearing double precision;
    length double precision := 0;
    total_time interval;
    start_time timestamp;
    end_time timestamp;
    geom_dump record;
begin
    for route in (select * from analysis.route_result where analysis.route_result.end_time - analysis.route_result.start_time between '3 minutes' and '1 hour' and cost_m > 250) loop
        total_time := route.end_time - route.start_time;
        start_time := route.start_time;
        end_time := route.end_time;
        for cell in select * from results.cell_9 loop
            if st_intersects(route.geom, cell.geom) then
                clipped_geom := st_intersection(route.geom, cell.geom);
                merged_geom := st_linemerge(clipped_geom);

                if st_geometrytype(merged_geom) = 'ST_LineString' then
                    -- Process as a single line string
                    segment_bearing := st_azimuth(st_startpoint(st_transform(merged_geom, 28992)), st_endpoint(st_transform(merged_geom, 28992)));
                    length := st_length(st_transform(merged_geom, 28992));

                    return query select 
                        route.trip_id,
                        segment_bearing,
                        segment_bearing * 180 / pi() as angle, -- angle from bearing
                        start_time,
                        end_time,
                        total_time,
                        length,
                        cell.index,
                        cell.bearing,
                        cell.angle,
                        cell.density,
                        cell.connectivity,
                        cell.bicycle_lane_density,
                        cell.primary_density,
                        cell.secondary_density,
                        cell.other_density,
                        cell.ped_density,
                        cell.dead_end_density,
                        merged_geom;
                else
                    -- Handle multiple disjoint line segments
					for geom_dump in select (st_dump(merged_geom)).geom loop
					    if st_geometrytype(geom_dump.geom) = 'ST_LineString' then
					        individual_geom := geom_dump.geom;
					        segment_bearing := st_azimuth(st_startpoint(st_transform(individual_geom, 28992)), st_endpoint(st_transform(individual_geom, 28992)));
					        length := st_length(st_transform(individual_geom, 28992));
					
					        return query select 
					            route.trip_id,
					            segment_bearing,
					            segment_bearing * 180 / pi() as angle, -- angle from bearing
					            start_time,
					            end_time,
					            total_time,
					            length,
					            cell.index,
					            cell.bearing,
					            cell.angle,
					            cell.density,
					            cell.connectivity,
					            cell.bicycle_lane_density,
					            cell.primary_density,
					            cell.secondary_density,
					            cell.other_density,
					            cell.ped_density,
					            cell.dead_end_density,
					            individual_geom;
					    end if;
					end loop;
                end if;
            end if;
        end loop;
    end loop;
end;
$$ language plpgsql;

create table results.cell_9_routes as (
SELECT * FROM analyze_routes() AS (
    trip_id INT,
    segment_bearing DOUBLE PRECISION,
    angle DOUBLE PRECISION,
    start_time timestamp,
    end_time timestamp,
    total_time interval,
    length double precision,
    cell_index TEXT,
    cell_bearing DOUBLE PRECISION,
    cell_angle DOUBLE PRECISION,
    cell_density DOUBLE PRECISION,
    cell_connectivity DOUBLE PRECISION,
    cell_bicycle_lane_density DOUBLE PRECISION,
    cell_primary_density DOUBLE PRECISION,
    cell_secondary_density DOUBLE PRECISION,
    cell_other_density DOUBLE PRECISION,
    cell_ped_density DOUBLE PRECISION,
    cell_dead_end_density DOUBLE PRECISION,
    segment_geom GEOMETRY
));






---
---
---

create or replace function analyze_routes_parallel(start_id int, end_id int)
returns table(
    trip_id int,
    segment_bearing double precision,
    angle double precision,
    start_time timestamp,
    end_time timestamp,
    total_time interval,
    length double precision,
    cell_index text,
    cell_bearing double precision,
    cell_angle double precision,
    cell_density double precision,
    cell_connectivity double precision,
    cell_bicycle_lane_density double precision,
    cell_primary_density double precision,
    cell_secondary_density double precision,
    cell_other_density double precision,
    cell_ped_density double precision,
    cell_dead_end_density double precision,
    segment_geom geometry
) as $$
declare
    route record;
    cell record;
    clipped_geom geometry;
    merged_geom geometry;
    individual_geom geometry;
    segment_bearing double precision;
    length double precision := 0;
    total_time interval;
    start_time timestamp;
    end_time timestamp;
    geom_dump record;
begin
    for route in select * from analysis.route_result 
                 where id >= start_id and id <= end_id
                 and analysis.route_result.duration between '3 minutes' and '1 hour' 
                 and cost_m > 250
    loop
        total_time := route.end_time - route.start_time;
        start_time := route.start_time;
        end_time := route.end_time;
        for cell in select * from results.cell_9 loop
            if st_intersects(route.geom, cell.geom) then
                clipped_geom := st_intersection(route.geom, cell.geom);
                merged_geom := st_linemerge(clipped_geom);

                if st_geometrytype(merged_geom) = 'ST_LineString' then
                    segment_bearing := st_azimuth(st_startpoint(st_transform(merged_geom, 28992)), st_endpoint(st_transform(merged_geom, 28992)));
                    length := st_length(st_transform(merged_geom, 28992));

                    return query select 
                        route.trip_id,
                        segment_bearing,
                        segment_bearing * 180 / pi() as angle,
                        start_time,
                        end_time,
                        total_time,
                        length,
                        cell.index,
                        cell.bearing,
                        cell.angle,
                        cell.density,
                        cell.connectivity,
                        cell.bicycle_lane_density,
                        cell.primary_density,
                        cell.secondary_density,
                        cell.other_density,
                        cell.ped_density,
                        cell.dead_end_density,
                        merged_geom;
                else
                    for geom_dump in select (st_dump(merged_geom)).geom loop
                        if st_geometrytype(geom_dump.geom) = 'ST_LineString' then
                            individual_geom := geom_dump.geom;
                            segment_bearing := st_azimuth(st_startpoint(st_transform(individual_geom, 28992)), st_endpoint(st_transform(individual_geom, 28992)));
                            length := st_length(st_transform(individual_geom, 28992));
                    
                            return query select 
                                route.trip_id,
                                segment_bearing,
                                segment_bearing * 180 / pi() as angle,
                                start_time,
                                end_time,
                                total_time,
                                length,
                                cell.index,
                                cell.bearing,
                                cell.angle,
                                cell.density,
                                cell.connectivity,
                                cell.bicycle_lane_density,
                                cell.primary_density,
                                cell.secondary_density,
                                cell.other_density,
                                cell.ped_density,
                                cell.dead_end_density,
                                individual_geom;
                        end if;
                    end loop;
                end if;
            end if;
        end loop;
    end loop;
end;
$$ language plpgsql;


SELECT * FROM analyze_routes_parallel(1, 10);


create table results.route_cell_stats_9 (
    trip_id int,
    segment_bearing double precision,
    angle double precision,
    start_time timestamp,
    end_time timestamp,
    total_time interval,
    length double precision,
    cell_index text,
    cell_bearing double precision,
    cell_angle double precision,
    cell_density double precision,
    cell_connectivity double precision,
    cell_bicycle_lane_density double precision,
    cell_primary_density double precision,
    cell_secondary_density double precision,
    cell_other_density double precision,
    cell_ped_density double precision,
    cell_dead_end_density double precision,
    segment_geom geometry
);

-- On analysis.route_result
create index idx_route_result_id on analysis.route_result(id);
create index idx_route_result_duration on analysis.route_result(duration);
create index idx_route_result_cost_m on analysis.route_result(cost_m);
create index idx_route_result_geom on analysis.route_result using gist(geom);

-- On results.cell_9
create index idx_cell_9_geom on results.cell_9 using gist(geom);



alter table results.route_cell_stats_9 add column cell_angle_normalized double precision;

update results.route_cell_stats_9
set
	angle_normalized = case
							when angle > 180
							then angle - 180
							else angle
						end;
						
create table results.route_cell_averages as (
select
	cell_index h3_index,
	avg(angle_normalized) mean_segment_angle,
	avg(length) mean_length,
	sum(length) sum_length,
	count(cell_index) segment_count,
	cell_angle_normalized cell_angle,
	cell_bicycle_lane_density,
	cell_primary_density,
	cell_secondary_density,
	cell_other_density,
	cell_ped_density,
	cell_dead_end_density,
	st_setsrid(h3_cell_to_boundary(cell_index::h3index)::geometry, 4326) as geom
from
	results.route_cell_stats_9
group by
	cell_index,
	cell_angle_normalized,
	cell_bicycle_lane_density,
	cell_primary_density,
	cell_secondary_density,
	cell_other_density,
	cell_ped_density,
	cell_dead_end_density
);
	
