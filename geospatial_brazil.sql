select * from geolocation where geolocation_city!='sao paulo' and geolocation_city!='são paulo';

ALTER TABLE geolocation ADD COLUMN geom geometry(Point, 4326);
UPDATE geolocation SET geom = ST_SetSRID(ST_MakePoint(geolocation_lng, geolocation_lat), 4326);

select geom from geolocation;

/* DISTANCE km; EARTH IS A PERFECT SPHERE */
SELECT ST_DistanceSphere(geometry(a.geom), geometry(b.geom))/1000
FROM geolocation a, geolocation b
WHERE a.geolocation_zip_code_prefix=5026 AND b.geolocation_zip_code_prefix=99990;

select max(geolocation_zip_code_prefix) from geolocation2;
select * from geolocation where geolocation_zip_code_prefix=99990;

create index zip_index on geolocation (geolocation_zip_code_prefix);
