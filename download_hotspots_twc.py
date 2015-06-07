import shapefile
import requests
import threading
import json
import xml2json
from xml2json import json2xml
import sqlite3 as lite

con = lite.connect('hotspots');
type_id = 2;

def get_hotspot(bbox):
    bbox_str = ','.join([str(b) for b in shape.bbox]);
    req_url = "http://coverage.twcwifi.com/fs/twc_wifi_hotspots?bbox="+bbox_str;
    r = requests.get(req_url);
    if(r.status_code == requests.codes.ok and len(r.json()['features']) > 0):
        """print req_url;"""
        features = r.json()['features'];
        num_features = len(features);
        for i in range(num_features):
            feature = features[i]["properties"];
            """create a hashkey for the hotspot"""
            hashkey = feature["ssid"]+","+feature["ap mac"];
            """check if this hotspot has already been downloaded"""
            check_sql = "Select count(*) from hotspots where ap_hash = '"+hashkey+"' and owner = 'twc'";
            cur = con.cursor();
            cur.execute(check_sql);
            count = cur.fetchone();
            """if yes, skip the next two steps"""
            if(count == 0):
                insert_sql = "Insert into hotspots ('owner', 'ap_hash') values ('twc','"+hashkey+"')";
                con.execute(insert_sql);
                print req_url;
                print feature.text;
                loc = "{lng:"+feature["long"]+", lat:"+feature["lat"]+"}";
                with open('twc_wifi_hotspots_pg.csv', 'a') as output_file:
                    """insert into mongodb and get the id"""
                    output_file.write(","+type_id+",Point("+feature["long"]+","+feature["lat"]+"),");

with open('twc_wifi_hotspots_pg.csv', 'w') as output_file:
    output_file.write("id,type_id,loc,doc_id" + '\n');
sf = shapefile.Reader("/home/Files/Counties/tl_2014_us_county.shp");
i = 0;
shape = sf.shape(i);
while True:
    bbox_arr = shape.bbox;
    get_hotspot(bbox_arr);
    i += 1;
    try:
        shape = sf.shape(i);
    except IndexError:
        break;
