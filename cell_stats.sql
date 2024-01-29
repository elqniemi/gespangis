create or replace function calculate_way_statistics(geohash_level int)
returns table(index text, bearing double precision, density double precision, geohash_geom geometry) as $$
declare
    cell record;
begin
    create temp table if not exists temp_geohash_cells as
    select distinct st_geohash(the_geom, geohash_level) as geohash_cell,
                    st_setsrid(st_geomfromgeohash(st_geohash(the_geom, geohash_level)), 4326) as cell_geom
    from network.ways_wgs;

    for cell in
        select geohash_cell, cell_geom from temp_geohash_cells
    loop
        index := cell.geohash_cell;
        geohash_geom := cell.cell_geom;
        bearing := (
            select coalesce(avg(st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom))), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        density := (
            select coalesce(count(*), 0) / nullif(st_area(cell.cell_geom), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        return next;
    end loop;
    drop table temp_geohash_cells;
end;
$$ language plpgsql;


-- new 

create or replace function calculate_way_stats(geohash_level int)
returns setof record as $$
declare
    cell record;
    result record;
begin
    -- Create a temporary table to store geohash cells with their geometries
    create temp table if not exists temp_geohash_cells as
    select distinct st_geohash(the_geom, geohash_level) as geohash_cell,
                    st_makevalid(st_setsrid(st_geomfromgeohash(st_geohash(the_geom, geohash_level), 4326), 4326)) as cell_geom
    from network.ways_wgs;

    for cell in
        select geohash_cell, cell_geom from temp_geohash_cells
    loop
        result.index := cell.geohash_cell;
        result.geohash_geom := cell.cell_geom;
        result.bearing := (
            select coalesce(avg(st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom))), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        result.density := (
            select coalesce(count(*), 0) / nullif(st_area(cell.cell_geom), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        
        return next result;
    end loop;

    -- Drop the temporary table
    drop table temp_geohash_cells;
end;
$$ language plpgsql;




---
---
--H3
---
---

create or replace function calculate_way_statistics(h3_level int)
returns table(index text, bearing double precision, density double precision, h3_geom geometry) as $$
declare
    cell record;
begin
    -- Create a temporary table to store H3 cells with their geometries
    create temp table if not exists temp_h3_cells as
    select distinct h3_lat_lng_to_cell(the_geom, h3_level) as h3_cell,
                    st_setsrid(h3_cell_to_boundary(h3_lat_lng_to_cell(the_geom, h3_level))::geometry, 4326) as cell_geom
    from network.ways_wgs_vertices;

    for cell in
        select h3_cell, cell_geom from temp_h3_cells
    loop
        index := cell.h3_cell;
        h3_geom := cell.cell_geom;
        bearing := (
            select coalesce(avg(st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom))), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        density := (
            select coalesce(count(*), 0) / nullif(st_area(cell.cell_geom::geography), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );
        return next;
    end loop;

    -- Drop the temporary table
    drop table temp_h3_cells;
end;
$$ language plpgsql;


create table network.ways_point as 
select
	st_centroid(the_geom)::point as the_geom
from 
	network.ways_wgs;
	



--- ALL STATS

create or replace function calculate_way_statistics_new(h3_level int)
returns table(index text, bearing double precision, density double precision, connectivity double precision, road_type_distribution json, bicycle_lane_density double precision, primary_density double precision, secondary_density double precision, other_density double precision, ped_density double precision, dead_end_density double precision, h3_geom geometry) as $$
declare
    cell record;
    rec record; -- Declare 'rec' as a record variable
    total_length double precision;
    weighted_bearing_sum double precision;
    weighted_bearing_sum_x double precision;
	weighted_bearing_sum_y double precision;
    road_bearing double precision;
    cul_de_sac_count int;
    bicycle_lane_length double precision;
    primary_length double precision;
    secondary_length double precision;
    other_length double precision;
    ped_length double precision;
    dead_end_count int;
    direction_count integer;
	main_direction_1 numeric;
	main_direction_2 numeric;
	deviation_sum numeric;
	segment_direction numeric;
	segment_deviation numeric;

begin
    -- Create a temporary table to store H3 cells with their geometries
    create temp table if not exists temp_h3_cells as
	with road_extent as (
	    select st_envelope(st_collect(the_geom))::polygon as geom
	    from network.ways_wgs
	)
	select distinct 
	    h3_cell,
	    st_setsrid(h3_cell_to_boundary(h3_cell)::geometry, 4326) as cell_geom
	from road_extent, 
	     lateral h3_polygon_to_cells(geom, NULL, 9) h3_cell;
	     
    for cell in
        select h3_cell, cell_geom from temp_h3_cells
    loop
        index := cell.h3_cell;
        h3_geom := cell.cell_geom;

        -- Weighted Bearing Calculation
        total_length := 0;
        weighted_bearing_sum := 0;
        weighted_bearing_sum_x := 0.0;
        weighted_bearing_sum_y := 0.0;

		for rec in
		    select length, st_azimuth(st_startpoint(the_geom), st_endpoint(the_geom)) as segment_bearing
		    from network.ways_wgs
		    where st_intersects(the_geom, cell.cell_geom)
		loop
		    if rec.segment_bearing is not null then
		        total_length := total_length + rec.length;
		        -- Convert bearing to vector components and accumulate
		        weighted_bearing_sum_x := weighted_bearing_sum_x + (rec.length * cos(rec.segment_bearing));
		        weighted_bearing_sum_y := weighted_bearing_sum_y + (rec.length * sin(rec.segment_bearing));
		    end if;
		end loop;
		
		-- Calculate average bearing from vector components
		bearing := case 
		    when total_length = 0 then null 
		    else atan2(weighted_bearing_sum_y, weighted_bearing_sum_x) 
		end;

        -- Density Calculation
        density := (
            select coalesce(sum(length), 0) / nullif(st_area(cell.cell_geom::geography), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );

        -- Connectivity (Average Segment Length)
        connectivity := (
            select coalesce(sum(length) / nullif(count(*), 0), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
        );

        -- Bicycle Lane Availability
        bicycle_lane_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (118, 201, 202, 203, 204) -- Replace 'bicycle_lane_tag' with the actual tag identifier for bicycle lanes
        );
        
        primary_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id > 100 and tag_id < 108
        );
        
        secondary_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (108, 124, 109, 125)
        );
        
        other_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (110, 111, 112, 113, 100, 123)
        );
        
        ped_length := (
            select coalesce(sum(length), 0)
            from network.ways_wgs
            where st_intersects(the_geom, cell.cell_geom)
            and tag_id in (114, 117, 119, 122)
        );
        
        -- Dead End Density Calculation
        dead_end_count := (
            select count(*)
            from network.ways_wgs_vertices
            where cnt = 1
            and st_intersects(geom, cell.cell_geom)
        );

        bicycle_lane_density := bicycle_lane_length / nullif(st_area(cell.cell_geom::geography), 0);
        primary_density := primary_length / nullif(st_area(cell.cell_geom::geography), 0);
        secondary_density := secondary_length / nullif(st_area(cell.cell_geom::geography), 0);
        other_density := other_length / nullif(st_area(cell.cell_geom::geography), 0);
        ped_density := ped_length / nullif(st_area(cell.cell_geom::geography), 0);
        dead_end_density := dead_end_count / nullif(st_area(cell.cell_geom::geography), 0);

        return next;
    end loop;

    -- Drop the temporary table
    drop table temp_h3_cells;
end
;
$$ language plpgsql;





---
---
---
---
create table results.cell_9 as (
select 
	index,
	bearing,
	bearing * 180 / pi() as angle,
	density,
	connectivity,
	bicycle_lane_density,
	primary_density,
	secondary_density,
	other_density,
	ped_density,
	dead_end_density,
	h3_geom as geom
from calculate_way_statistics_new(9));

create table network.ways_wgs as (
	select
		gid,
		tag_id,
		st_length(the_geom) length,
		st_transform(the_geom, 4326) the_geom
	from
		network.ways);

create index idx_network_ways_wgs_geom on network.ways_wgs using gist(the_geom);
create index idx_network_ways_wgs_vertices_geom on network.ways_wgs_vertices using gist(geom);