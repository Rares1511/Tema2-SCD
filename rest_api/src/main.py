import flask
from datetime import datetime
from flask import request, jsonify
import pymongo as mongo
from pymongo import errors

app = flask.Flask(__name__)
mongo_client = mongo.MongoClient('mongodb://root:example@scd_mongodb:27017/admin')
db = mongo_client['scd_tema2']


############################# UTILS #############################


class ColumnNames:
    ID = 'id'
    NUME = 'nume'
    LAT = 'lat'
    LON = 'lon'
    ID_TARA = 'idTara'
    VALOARE = 'valoare'
    TIMESTAMP = 'timestamp'
    ID_ORAS = 'idOras'
    FROM = 'from'
    UNTIL = 'until'

MAX_ID = 1000000

country_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.NUME, ColumnNames.LAT, ColumnNames.LON],
        "properties": {
            ColumnNames.ID: { "bsonType": "int" },
            ColumnNames.NUME: { "bsonType": "string" },
            ColumnNames.LAT: { "bsonType": ["int", "double"] },
            ColumnNames.LON: { "bsonType": ["int", "double"] }
        }
    }
}

city_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.ID_TARA, ColumnNames.NUME, ColumnNames.LAT, ColumnNames.LON],
        "properties": {
            ColumnNames.ID: { "bsonType": "int" },
            ColumnNames.ID_TARA: { "bsonType": "int" },
            ColumnNames.NUME: { "bsonType": "string" },
            ColumnNames.LAT: { "bsonType": ["int", "double"] },
            ColumnNames.LON: { "bsonType": ["int", "double"] }
        }
    }
}

temperature_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.VALOARE, ColumnNames.TIMESTAMP, ColumnNames.ID_ORAS],
        "properties": {
            ColumnNames.ID: { "bsonType": "int" },
            ColumnNames.VALOARE: { "bsonType": ["int", "double"] },
            ColumnNames.TIMESTAMP: { "bsonType": "date" },
            ColumnNames.ID_ORAS: { "bsonType": "int" }
        }
    }
}


def get_first_unused_id(collection) -> int:
    for i in range(MAX_ID):
        if db[collection].find_one({ColumnNames.ID: i}) is None:
            return i
    return -1


############################# COUNTRIES #############################


@app.route('/api/countries', methods=['POST'])
def api_countries_post():
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = get_first_unused_id('countries')
        db['countries'].insert_one(request_data)
    except errors.DuplicateKeyError as e:
        return 'A country with the same name already exists', 409
    except:
        return 'Bad Request', 400
    return jsonify({ColumnNames.ID: request_data[ColumnNames.ID]}), 201

@app.route('/api/countries', methods=['GET'])
def api_countries_get():
    countries = list(db['countries'].find({}, {'_id': 0}))
    return jsonify(countries), 200

@app.route('/api/countries/<int:id>', methods=['PUT'])
def api_countries_put(id : int):
    request_data = request.get_json()
    try:
        # Check if the country to be updated exists
        if db['countries'].find_one({ColumnNames.ID: id}) is None:
            return 'Country not found', 404
        # Try to update the country with the new data
        db['countries'].replace_one({ColumnNames.ID: id}, request_data)
    except errors.DuplicateKeyError as e:
        return 'A country with the same name/id already exists', 409
    except:
        return 'Bad Request', 400
    return 'Success', 200

@app.route('/api/countries/<int:id>', methods=['DELETE'])
def api_countries_delete(id : int):
    # Check if the country to be deleted exists
    if db['countries'].find_one({ColumnNames.ID: id}) is None:
        return 'Not found', 404
    # Delete the country
    cities = list(db['cities'].find({ColumnNames.ID_TARA: id}, {ColumnNames.ID: 1}))
    for city in cities:
        db['temperatures'].delete_many({ColumnNames.ID_ORAS: city[ColumnNames.ID]})
    db['cities'].delete_many({ColumnNames.ID_TARA: id})
    db['countries'].delete_one({ColumnNames.ID: id})
    return 'Success', 200


############################# CITIES #############################


@app.route('/api/cities', methods=['POST'])
def api_cities_post():
    request_data = request.get_json()
    try:
        # Assign an unique id to the city
        request_data[ColumnNames.ID] = get_first_unused_id('cities')
        # Check if the country of the city exists
        if db['countries'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_TARA]}) is None:
            return 'Country not found', 404
        # Insert the city in the database
        db['cities'].insert_one(request_data)
    except errors.DuplicateKeyError as e:
        return 'A city with the same name already exists', 409
    except:
        return 'Bad Request', 400
    return jsonify({ColumnNames.ID: request_data[ColumnNames.ID]}), 201

@app.route('/api/cities', methods=['GET'])
def api_cities_get():
    cities = list(db['cities'].find({}, {'_id': 0}))
    return jsonify(cities), 200

@app.route('/api/cities/<int:id>', methods=['GET'])
def api_cities_get_id(id : int):
    return jsonify(db['cities'].find_one({ColumnNames.ID: id}, {'_id': 0})), 200

@app.route('/api/cities/country/<int:id>', methods=['GET'])
def api_cities_get_country_id(id : int):
    return jsonify(list(db['cities'].find({ColumnNames.ID_TARA: id}, {'_id': 0}))), 200

@app.route('/api/cities/<int:id>', methods=['PUT'])
def api_cities_put_id(id : int):
    try:
        request_data = request.get_json()
        # Check if the city to be updated exists
        if db['cities'].find_one({ColumnNames.ID: id}) is None:
            return 'City not found', 404
        # Check if the new country of the city exists
        if db['countries'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_TARA]}) is None:
            return 'Country not found', 404
        # Update the city with the new data
        db['cities'].replace_one({ColumnNames.ID: id}, request_data)
    except errors.DuplicateKeyError as e:
        return 'A city with the same name/id already exists', 409
    except:
        return 'Bad Request', 400
    return 'Success', 200

@app.route('/api/cities/<int:id>', methods=['DELETE'])
def api_cities_delete_id(id : int):
    if db['cities'].find_one({ColumnNames.ID: id}) is None:
        return 'Not found', 404
    db['temperatures'].delete_many({ColumnNames.ID_ORAS: id})
    db['cities'].delete_one({ColumnNames.ID: id})
    return 'Success', 200


############################# TEMPERATURES #############################


@app.route('/api/temperatures', methods=['POST'])
def api_temperatures_post():
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = get_first_unused_id('temperatures')
        request_data[ColumnNames.TIMESTAMP] = datetime.now()
        if db['cities'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_ORAS]}) is None:
            return 'City not found', 404
        db['temperatures'].insert_one(request_data)
    except errors.DuplicateKeyError as e:
        return 'A temperature with the same city and timestamp already exists', 409
    except:
        return 'Bad Request', 400
    return jsonify({ColumnNames.ID: request_data[ColumnNames.ID]}), 201

@app.route('/api/temperatures', methods=['GET'])
def api_temperatures_get():
    lat = request.args.get(ColumnNames.LAT)
    lon = request.args.get(ColumnNames.LON)
    from_date = request.args.get(ColumnNames.FROM)
    until_date = request.args.get(ColumnNames.UNTIL)
    temperatures_filter = {}
    cities_filter = {}
    if lat is not None:
        cities_filter[ColumnNames.LAT] = float(lat)
    if lon is not None:
        cities_filter[ColumnNames.LON] = float(lon)
    cities = [city[ColumnNames.ID] for city in list(db['cities'].find(cities_filter, {'_id': 0}))]
    temperatures_filter[ColumnNames.ID_ORAS] = {'$in': cities}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$gte': datetime.strptime(from_date, '%Y-%m-%d')}
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$lte': datetime.strptime(until_date, '%Y-%m-%d')}
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0, ColumnNames.ID_ORAS: 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/cities/<int:idOras>', methods=['GET'])
def api_temperatures_get_city_id(idOras : int):
    from_date = request.args.get(ColumnNames.FROM)
    until_date = request.args.get(ColumnNames.UNTIL)
    temperatures_filter = {ColumnNames.ID_ORAS: idOras}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$gte': datetime.strptime(from_date, '%Y-%m-%d')}
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$lte': datetime.strptime(until_date, '%Y-%m-%d')}
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0, ColumnNames.ID_ORAS: 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/countries/<int:idTara>', methods=['GET'])
def api_temperatures_get_country_id(idTara : int):
    from_date = request.args.get(ColumnNames.FROM)
    until_date = request.args.get(ColumnNames.UNTIL)
    temperatures_filter = {}
    cities = [city[ColumnNames.ID] for city in list(db['cities'].find({ColumnNames.ID_TARA: idTara}, {'_id': 0}))]
    temperatures_filter[ColumnNames.ID_ORAS] = {'$in': cities}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$gte': datetime.strptime(from_date, '%Y-%m-%d')}
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = {'$lte': datetime.strptime(until_date, '%Y-%m-%d')}
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0, ColumnNames.ID_ORAS: 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/<int:id>', methods=['PUT'])
def api_temperatures_put_id(id : int):
    request_data = request.get_json()
    try:
        request_data[ColumnNames.TIMESTAMP] = datetime.now()
        # Check if the temperature to be updated exists
        if db['temperatures'].find_one({ColumnNames.ID: id}) is None:
            return 'Temperature Not found', 404
        # Check if the new city of the temperature exists
        if db['cities'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_ORAS]}) is None:
            return 'City not found', 404
        db['temperatures'].replace_one({ColumnNames.ID: id}, request_data)
    except errors.DuplicateKeyError as e:
        return 'A temperature with the same (city and timestamp)/id already exists', 409
    except:
        return 'Bad Request', 400
    return 'Success', 200

@app.route('/api/temperatures/<int:id>', methods=['DELETE'])
def api_temperatures_delete_id(id : int):
    if db['temperatures'].find_one({ColumnNames.ID: id}) is None:
        return 'Not found', 404
    db['temperatures'].delete_one({ColumnNames.ID: id})
    return 'Success', 200


############################# MAIN #############################


def init():
    if 'countries' not in db.list_collection_names():
        db.create_collection('countries', validator=country_validation)
        db['countries'].create_index([(ColumnNames.NUME, mongo.TEXT)], unique=True)
        db['countries'].create_index([(ColumnNames.ID, mongo.ASCENDING)], unique=True)
    if 'cities' not in db.list_collection_names():
        db.create_collection('cities', validator=city_validation)
        db['cities'].create_index([(ColumnNames.NUME, mongo.TEXT)], unique=True)
        db['cities'].create_index([(ColumnNames.ID, mongo.ASCENDING)], unique=True)
    if 'temperatures' not in db.list_collection_names():
        db.create_collection('temperatures', validator=temperature_validation)
        db['temperatures'].create_index([(ColumnNames.ID_ORAS, mongo.TEXT), (ColumnNames.TIMESTAMP, mongo.TEXT)], unique=True)
        db['temperatures'].create_index([(ColumnNames.ID, mongo.ASCENDING)], unique=True)

if __name__ == '__main__':
    init()
    
    app.run('0.0.0.0', debug=True, port=80)