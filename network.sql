select pgr_createtopology('network.ways', 0.001, clean:=true);

create table network.ways_wgs_vertices as 
select
	cnt, 
	st_transform(the_geom, 4326)::point the_geom,
	st_transform(the_geom, 4326) geom
from
	network.ways_vertices_pgr;
	
select pgr_analyzeGraph('network.ways', 0.0001);
