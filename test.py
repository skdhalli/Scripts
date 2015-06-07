import pymongo
from pymongo import MongoClient

client = MongoClient();
db = client.hotspots;
hotspots_coll = db.twc_hotspots;
json = { "ueid" : "356567055753949", "model" : "SAMSUNG-SGH-I337", "manufacturer" : "samsung" };
hotspot_id = hotspots_coll.insert_one(json).inserted_id;
print hotspot_id;