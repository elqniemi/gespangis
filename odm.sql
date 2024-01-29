create or replace function generate_od_matrix(trips_table_schema text, trips_table_name text, vertices_table_schema text, vertices_table_name text, srid int)
returns table(trip_id int, source_vertex int, target_vertex int) as $$
declare
    rec record;
    source_id int;
    target_id int;
begin
    for rec in execute format('select trip_id, geom_start, geom_end from %I.%I', trips_table_schema, trips_table_name)
    loop
        -- find nn for source
        execute format('select id from %I.%I order by the_geom <-> st_setsrid(st_makepoint(%s, %s), %s) limit 1', 
                       vertices_table_schema, vertices_table_name, st_x(rec.geom_start), st_y(rec.geom_start), srid)
        into source_id;

        -- find nn for target
        execute format('select id from %I.%I order by the_geom <-> st_setsrid(st_makepoint(%s, %s), %s) limit 1', 
                       vertices_table_schema, vertices_table_name, st_x(rec.geom_end), st_y(rec.geom_end), srid)
        into target_id;

        -- assign values
        trip_id := rec.trip_id;
        source_vertex := source_id;
        target_vertex := target_id;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;



create or replace function generate_od_matrix(
    trips_table_schema text, 
    trips_table_name text, 
    vertices_table_schema text, 
    vertices_table_name text, 
    srid int
)
returns table(trip_id int, source_vertex int, target_vertex int) as $$
declare
    rec record;
    source_id int;
    target_id int;
begin
    for rec in execute format('select trip_id, geom_start, geom_end from %I.%I', trips_table_schema, trips_table_name)
    loop
        -- find nn for source with filtering edges
        execute format($f$
            select v.id 
            from 
                %I.%I v
                join network.ways w on v.id = w.source or v.id = w.target
            where w.tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)
            order by v.the_geom <-> st_setsrid(st_makepoint(%s, %s), %s)
            limit 1
            $f$, 
            vertices_table_schema, vertices_table_name, 
            st_x(rec.geom_start), st_y(rec.geom_start), srid
        ) into source_id;

        -- find nn for target with filtering edges
        execute format($f$
            select v.id 
            from 
                %I.%I v
                join network.ways w on v.id = w.source or v.id = w.target
            where w.tag_id not in (101, 102, 103, 104, 105, 113, 114, 115, 116, 117, 119, 120, 121, 122, 123)
            order by v.the_geom <-> st_setsrid(st_makepoint(%s, %s), %s)
            limit 1
            $f$, 
            vertices_table_schema, vertices_table_name, 
            st_x(rec.geom_end), st_y(rec.geom_end), srid
        ) into target_id;

        -- assign values
        trip_id := rec.trip_id;
        source_vertex := source_id;
        target_vertex := target_id;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

create table analysis.odm as 
select 
	trip_id,
	source_vertex as source,
	target_vertex as target
from
	generate_od_matrix('scooters', 'trips', 'network', 'ways_vertices_pgr', '28992');
	
create index idx_ways_vertices_pgr on network.ways_vertices_pgr using gist(the_geom);
create index idx_trips_start on scooters.trips using gist(geom_start);
create index idx_trips_end on scooters.trips using gist(geom_end);