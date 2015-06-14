import pymongo
import requests
import xml.etree.ElementTree as ET
from pymongo import MongoClient
import xmltodict, json
from time import sleep
from time import gmtime, strftime

host = "starbucks";
wait_sec = 5;
max_retry_count = 5;

def getCurrentTimeStamp():
    return strftime("%Y-%m-%d %H:%M:%S", gmtime());

def log_hotspot_ds(json_obj):
    hotspot_id = -1;
    with MongoClient() as client:
        db = client.hotspots;
        hotspots_coll = db.sb_hotspots;
        hotspot_id = hotspots_coll.insert_one(json_obj).inserted_id;
    return hotspot_id;

def log_hotspot_pg(zipcode):
    succeeded = False;
    retry = 0;
    while(not succeeded):
        try:
            req_url = "http://testhost.openapi.starbucks.com/location/v2/stores/postal?postalcode="+str(zipcode);
            r = requests.get(req_url);
            succeeded =True;
        except:
            succeeded = False;
            retry += 1;
            if(retry > max_retry_count):
                print getCurrentTimeStamp()+": Failed to get hotspots for "+str(zipcode)+" after "+str(max_retry_count)+" attempts";
                break;
            else:
                print getCurrentTimeStamp()+": Connection failed. Retry attempt # "+str(retry);
                print getCurrentTimeStamp()+": Waiting for "+str(wait_sec)+" seconds before retry ..";
                sleep(wait_sec);
    
    
    if(succeeded and (r.status_code == requests.codes.ok)):
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
                mongoid = log_hotspot_ds(o);
                with open('sb_hotspots.csv', 'a') as output_file:
                    output_file.write(host+","+latitude+","+longitude+","+str(mongoid)+'\n');
        print getCurrentTimeStamp()+": Finished processing zip code - "+str(zipcode);

print getCurrentTimeStamp()+": ****SCRIPT STARTED"+"****";

with open('sb_hotspots.csv', 'w') as output_file:
	output_file.write("host,latitude,longitude,doc_id"+'\n');

with MongoClient() as client:
    db = client.hotspots;
    hotspots_coll = db.sb_hotspots;
    hotspots_coll.remove(None);
    zipcodes = db.zipcodes.distinct('Zipcode');

for zipcode in zipcodes:
    print getCurrentTimeStamp()+": Started processing zip code - "+str(zipcode);
    log_hotspot_pg(zipcode);

print getCurrentTimeStamp()+": ****SCRIPT ENDED"+"****";
	
