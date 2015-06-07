import pymongo
import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
import xmltodict, json

type_id = 1;
client = MongoClient();
db = client.hotspots;
hotspots_coll = db.sb_hotspots;

def log_hotspot_ds(json_obj):
    hotspot_id = hotspots_coll.insert_one(json_obj).inserted_id;
    return hotspot_id;

def log_hotspot_pg(zipcode):
	req_url = "http://testhost.openapi.starbucks.com/location/v2/stores/postal?postalcode="+str(zipcode);
	r = requests.get(req_url);
	if(r.status_code == requests.codes.ok):
		output = -1;
		root = ET.fromstring(r.content);
        stores = root[1];
        count = 0;
        for store in stores:
            count += 1;
            """print ET.tostring(store);"""
            o = xmltodict.parse(ET.tostring(store, encoding='utf-8', method='xml'));
            if(("ns0:store" in o) and ("ns0:coordinates" in o["ns0:store"]) and ("ns0:latitude" in o["ns0:store"]["ns0:coordinates"])):
                latitude = o["ns0:store"]["ns0:coordinates"]["ns0:latitude"];
                longitude = o["ns0:store"]["ns0:coordinates"]["ns0:longitude"];
                loc = "Point("+longitude+","+latitude+")";
                mongoid = log_hotspot_ds(o);
                with open('sb_hotspots.csv', 'a') as output_file:
                    output_file.write(","+type_id+","+loc+","+mongoid+'\n');

with open('sb_hotspots.csv', 'w') as output_file:
	output_file.write("id,type_id,loc,doc_id"+'\n');

client = MongoClient();
db = client.hotspots;
zipcodes = db.zipcodes.distinct('Zipcode');
for zipcode in zipcodes:
	log_hotspot_pg(zipcode);
	
