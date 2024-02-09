import os

# Get it from security/database/users and run "export MONGODB_PWD=PWD"
MONGODB_PWD = os.environ['MONGODB_PWD']

from pymongo import MongoClient

# Replace the uri string with your MongoDB deployment's connection string.
uri = f"mongodb+srv://nicoalar22:{MONGODB_PWD}@nalvarezcluster.i35xfxb.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(uri)

# database and collection code goes here
db = client.sample_guides
coll_planets = db.planets

# Retrieved all documents from the sample_guides.planets collection
# without specifying any criteria that the documents should meet.
cursor = coll_planets.find()
print("\nAll the Planets")
for doc in cursor:
    print(doc)

# Retrieved all documents from the sample_guides.planets collection where "hasRings" is True.
cursor = coll_planets.find({"hasRings": True})
print("\nPlanets with rings")
for doc in cursor:
    print(doc)

# Retrieved all documents from the sample_guides.planets collection where "hasRings" is False
# and has "Ar" as mainAtmosphere.
cursor = coll_planets.find({"hasRings": False, "mainAtmosphere": "Ar"})
print("\nPlanets without rings and with Ar in their atmosphere")
for doc in cursor:
    print(doc)

# Use  dot notation  in this query to select documents where the embedded document
# surfaceTemperatureC has a value in its mean field less than 15 degrees (Celsius)
print("\nPlanets with surfaceTemperatureC lower than 15 degrees")
cursor = coll_planets.find({"surfaceTemperatureC.mean": {"$lt": 15}})
for doc in cursor:
    print(doc)

# Retrieves all documents in the planets collection where the surfaceTemperatureC.mean
# field is less than 15 and the surfaceTemperatureC.min field is greater than -100.
print("\nPlanets with: surfaceTemperatureC.mean lt 15, surfaceTemperatureC.min gt -100")
cursor = coll_planets.find(
    {"surfaceTemperatureC.mean": {"$lt": 15}, "surfaceTemperatureC.min": {"$gt": -100}}  # Implicit AND
)
for doc in cursor:
    print(doc)


# If you do not use the $and operator, the driver encounters the same key multiple
# times in the query filter, and uses the last key encountered.
print("\nPlanets with: orderFromSun gt 2, orderFromSun lt 5")
cursor = coll_planets.find(
    {"$and": [{"orderFromSun": {"$gt": 2}}, {"orderFromSun": {"$lt": 5}}]}  # Explicit AND
)
for doc in cursor:
    print(doc)

# Match documents in the planets collection where the orderFromSun value
# is both greater than 7 AND less than 2
print("\nPlanets with: orderFromSun greater than 7 OR less than 2")
cursor = coll_planets.find(
    {
        "$or": [
            {"orderFromSun": {"$gt": 7}},
            {"orderFromSun": {"$lt": 2}},
        ]
    }
)
for doc in cursor:
    print(doc)


# Insert documents into the comets collection
# If you omit the _id field, the driver automatically generates a unique ObjectId value for the _id field.
coll_comets = db.comets
docs = [
    {"name": "Halley's Comet", "officialName": "1P/Halley", "orbitalPeriod": 75, "radius": 3.4175, "mass": 2.2e14},
	{"name": "Wild2", "officialName": "81P/Wild", "orbitalPeriod": 6.41, "radius": 1.5534, "mass": 2.3e13},
	{"name": "Comet Hyakutake", "officialName": "C/1996 B2", "orbitalPeriod": 17000, "radius": 0.77671, "mass": 8.8e12},
    ]
result = coll_comets.insert_many(docs)
print("\nInserted comets")
print(result.inserted_ids)

# Update to convert the radius field from the metric system to the imperial system in all documents.
doc = {
    '$mul': {'radius': 1.60934}
}
result = coll_comets.update_many({}, doc)
print("\nUpdated comets")
print("Number of documents updated: ", result.modified_count)

# Delete documents where their orbitalPeriod is greater than 5 and less than 85.
doc = {
    "orbitalPeriod": {
        "$gt": 5,
        "$lt": 85
    }
}
result = coll_comets.delete_many(doc)
print("Number of documents deleted: ", result.deleted_count)

# Close the connection to MongoDB when you're done.
client.close()
