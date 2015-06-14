delete from ap_locations where host='starbucks';

copy ap_locations (host,latitude,longitude,doc_id) from '/home/Scripts/starbucks/sb_hotspots.csv' with delimiter as ',' csv header;

update ap_locations set loc = ST_SetSRID(ST_Point(longitude,latitude),4326);

