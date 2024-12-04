import flask
from datetime import datetime
from flask import request, jsonify
from pymongo import MongoClient, TEXT

class ColumnNames:
    ID = 'id'
    NUME = 'nume'
    LAT = 'lat'
    LON = 'lon'
    ID_TARA = 'idTara'
    VALOARE = 'valoare'
    TIMESTAMP = 'timestamp'
    ID_ORAS = 'id_oras'

MAX_ID = 1000000

app = flask.Flask(__name__)
mongo_client = MongoClient('mongodb://root:example@scd_mongodb:27017/admin')
db = mongo_client['scd_tema2']
country_id_map = {}
cities_id_map = {}
temperature_id_map = {}

country_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.NUME, ColumnNames.LAT, ColumnNames.LON],
        "properties": {
            ColumnNames.ID: {
                "bsonType": "int",
                "description": "must be an integer and is required"
            },
            ColumnNames.NUME: {
                "bsonType": "string",
                "description": "must be a string and is required"
            },
            ColumnNames.LAT: {
                "bsonType": ["int", "double"],
                "description": "must be a double and is required"
            },
            ColumnNames.LON: {
                "bsonType": ["int", "double"],
                "description": "must be a double and is required"
            }
        }
    }
}

city_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.ID_TARA, ColumnNames.NUME, ColumnNames.LAT, ColumnNames.LON],
        "properties": {
            ColumnNames.ID: {
                "bsonType": "int",
                "description": "must be an integer and is required"
            },
            ColumnNames.ID_TARA: {
                "bsonType": "int", 
                "description": "must be an integer and is required"
            },
            ColumnNames.NUME: {
                "bsonType": "string",
                "description": "must be a string and is required"
            },
            ColumnNames.LAT: {
                "bsonType": ["int", "double"],
                "description": "must be a double and is required"
            },
            ColumnNames.LON: {
                "bsonType": ["int", "double"],
                "description": "must be a double and is required"
            }
        }
    }
}

temperature_validation = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": [ColumnNames.ID, ColumnNames.VALOARE, ColumnNames.TIMESTAMP, ColumnNames.ID_ORAS],
        "properties": {
            ColumnNames.ID: {
                "bsonType": "int",
                "description": "must be an integer and is required"
            },
            ColumnNames.VALOARE: {
                "bsonType": ["int", "double"],
                "description": "must be a string and is required"
            },
            ColumnNames.TIMESTAMP: {
                "bsonType": "date"
            },
            ColumnNames.ID_ORAS: {
                "bsonType": "int",
                "description": "must be an integer and is required"
            }
        }
    }
}

def get_first_unused_id(map) -> int:
    for i in range(MAX_ID):
        if i not in map:
            return i
    return -1

@app.before_first_request
def before_first_request_func():
    if 'countries' not in db.list_collection_names():
        db.create_collection('countries', validator=country_validation)
    if 'cities' not in db.list_collection_names():
        db.create_collection('cities', validator=city_validation)
    if 'temperatures' not in db.list_collection_names():
        db.create_collection('temperatures', validator=temperature_validation)
    
    db['countries'].create_index([(ColumnNames.NUME, TEXT)], unique=True)
    db['cities'].create_index([(ColumnNames.NUME, TEXT)], unique=True)
    db['temperatures'].create_index([(ColumnNames.ID_ORAS, TEXT), ('timstamp', TEXT)], unique=True)
    
    countries = list(db['countries'].find({}, {'_id': 0}))
    for country in countries:
        country_id_map[country[ColumnNames.ID]] = country[ColumnNames.NUME]
        
    cities = list(db['cities'].find({}, {'_id': 0}))
    for city in cities:
        cities_id_map[city[ColumnNames.ID]] = city[ColumnNames.NUME]
        
    temperatures = list(db['temperatures'].find({}, {'_id': 0}))
    for temperature in temperatures:
        temperature_id_map[temperature[ColumnNames.ID]] = temperature[ColumnNames.ID_ORAS]


############################# COUNTRIES #############################


@app.route('/api/countries', methods=['POST'])
def api_countries_post():
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = get_first_unused_id(country_id_map)
        db['countries'].insert_one(request_data)
        country_id_map[request_data[ColumnNames.ID]] = request_data[ColumnNames.NUME]
    except:
        return 'Conflict', 409
    return jsonify({ColumnNames.ID: request_data[ColumnNames.ID]}), 201

@app.route('/api/countries', methods=['GET'])
def api_countries_get():
    countries = list(db['countries'].find({}, {'_id': 0}))
    return jsonify(countries), 200

@app.route('/api/countries/<int:id>', methods=['PUT'])
def api_countries_put(id : int):
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = id
        db['countries'].update_one({ColumnNames.ID: id}, {'$set': request_data})
    except:
        return 'Conflict', 400
    return 'Success', 200

@app.route('/api/countries/<int:id>', methods=['DELETE'])
def api_countries_delete(id : int):
    try:
        db['countries'].delete_one({ColumnNames.ID: id})
        country_id_map.pop(id)
    except:
        return 'Not found', 404
    return 'Success', 200


############################# CITIES #############################


@app.route('/api/cities', methods=['POST'])
def api_cities_post():
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = get_first_unused_id(cities_id_map)
        if db['countries'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_TARA]}) is None:
            return 'Country not found', 404
        db['cities'].insert_one(request_data)
        cities_id_map[request_data[ColumnNames.ID]] = request_data[ColumnNames.NUME]
    except:
        return 'Conflict', 409
    return 'Success', 201

@app.route('/api/cities', methods=['GET'])
def api_cities_get():
    cities = list(db['cities'].find({}, {'_id': 0}))
    return jsonify(cities), 200

@app.route('/api/cities/<int:id>', methods=['GET'])
def api_cities_get_id(id : int):
    city = db['cities'].find_one({ColumnNames.ID: id}, {'_id': 0})
    if city is None:
        return 'Not found', 404
    return jsonify(city), 200

@app.route('/api/cities/country/<int:id>', methods=['GET'])
def api_cities_get_country_id(id : int):
    cities = list(db['cities'].find({ColumnNames.ID_TARA: id}, {'_id': 0}))
    return jsonify(cities), 200

@app.route('/api/cities/<int:id>', methods=['PUT'])
def api_cities_put_id(id : int):
    try:
        request_data = request.get_json()
        request_data[ColumnNames.ID] = id
        if db['countries'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_TARA]}) is None:
            return 'Country not found', 404
        db['cities'].update_one({ColumnNames.ID: id}, {'$set': request_data})
    except:
        return 'Conflict', 400
    return 'Success', 200

@app.route('/api/cities/<int:id>', methods=['DELETE'])
def api_cities_delete_id(id : int):
    try:
        db['cities'].delete_one({ColumnNames.ID: id})
        cities_id_map.pop(id)
    except:
        return 'Not found', 404
    return 'Success', 200


############################# TEMPERATURES #############################


@app.route('/api/temperatures', methods=['POST'])
def api_temperatures_post():
    request_data = request.get_json()
    try:
        if db['cities'].find_one({ColumnNames.ID: request_data[ColumnNames.ID_ORAS]}) is None:
            return 'City not found', 404
        request_data[ColumnNames.ID] = get_first_unused_id(temperature_id_map)
        request_data[ColumnNames.TIMESTAMP] = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        db['temperatures'].insert_one(request_data)
        temperature_id_map[request_data[ColumnNames.ID]] = request_data[ColumnNames.ID_ORAS]
    except:
        return 'Conflict', 409
    return 'Success', 201

@app.route('/api/temperatures', methods=['GET'])
def api_temperatures_get():
    lat = request.args.get(ColumnNames.LAT)
    lon = request.args.get(ColumnNames.LON)
    from_date = request.args.get('from')
    until_date = request.args.get('until')
    temperatures_filter = {}
    cities_filter = {}
    if lat is not None:
        cities_filter[ColumnNames.LAT] = float(lat)
    if lon is not None:
        cities_filter[ColumnNames.LON] = float(lon)
    cities = [city[ColumnNames.ID] for city in list(db['cities'].find(cities_filter, {'_id': 0}))]
    temperatures_filter[ColumnNames.ID_ORAS] = {'$in': cities}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$gte': datetime.strptime(from_date, '%Y-%m-%d') }
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$lte': datetime.strptime(until_date, '%Y-%m-%d') }
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/cities/<int:id_oras>', methods=['GET'])
def api_temperatures_get_city_id(id_oras : int):
    from_date = request.args.get('from')
    until_date = request.args.get('until')
    temperatures_filter = {ColumnNames.ID_ORAS: id_oras}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$gte': datetime.strptime(from_date, '%Y-%m-%d') }
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$lte': datetime.strptime(until_date, '%Y-%m-%d') }
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/countries/<int:idTara>', methods=['GET'])
def api_temperatures_get_country_id(idTara : int):
    from_date = request.args.get('from')
    until_date = request.args.get('until')
    temperatures_filter = {}
    cities = [city[ColumnNames.ID] for city in list(db['cities'].find({ColumnNames.ID_TARA: idTara}, {'_id': 0}))]
    temperatures_filter[ColumnNames.ID_ORAS] = {'$in': cities}
    if from_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$gte': datetime.strptime(from_date, '%Y-%m-%d') }
    if until_date is not None:
        temperatures_filter[ColumnNames.TIMESTAMP] = { '$lte': datetime.strptime(until_date, '%Y-%m-%d') }
    temperatures = [{**temperature, ColumnNames.TIMESTAMP: temperature[ColumnNames.TIMESTAMP].strftime('%Y-%m-%d')} 
                    for temperature in list(db['temperatures'].find(temperatures_filter, {'_id': 0}))]
    return jsonify(temperatures), 200

@app.route('/api/temperatures/<int:id>', methods=['PUT'])
def api_temperatures_put_id(id : int):
    request_data = request.get_json()
    try:
        request_data[ColumnNames.ID] = id
        db['temperatures'].update_one({ColumnNames.ID: id}, {'$set': request_data})
    except:
        return 'Conflict', 400
    return 'Success', 200

@app.route('/api/temperatures/<int:id>', methods=['DELETE'])
def api_temperatures_delete_id(id : int):
    try:
        db['temperatures'].delete_one({ColumnNames.ID: id})
        temperature_id_map.pop(id)
    except:
        return 'Not found', 404
    return 'Success', 200


############################# MAIN #############################


if __name__ == '__main__':
    app.run('0.0.0.0', debug=True)