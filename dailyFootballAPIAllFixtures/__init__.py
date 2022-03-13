import logging
import os
import traceback
import http.client
import json
import bson
from datetime import datetime
import pathlib
from azure.storage.blob import ContainerClient
import azure.functions as func
import pymongo

global json_i_want
global fixtures_list
global all_fixtures
global number_of_erros

# https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable/
class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

# https://stackoverflow.com/a/55080038
def object_id_from_int(n):
    s = str(n)
    s = '0' * (24 - len(s)) + s
    return bson.ObjectId(s)

# https://stackoverflow.com/a/55080038
def int_from_object_id(obj):
    return int(str(obj))


def get_folder(filename):
    current_path = pathlib.Path(__file__).parent.parent
    return str(current_path / filename)

def check_api(key, request):
    connection = http.client.HTTPSConnection("v3.football.api-sports.io")

    headers = {
        'x-rapidapi-host': "v3.football.api-sports.io",
        'x-rapidapi-key': key
    }

    connection.request("GET", request, headers=headers)

    response = connection.getresponse()
    data_recieved = response.read()

    return data_recieved


def process_api_data(data):
    json_i_want = {}
    json_i_want["FIXTURES"] = []
    football_api_key = os.getenv('footballapikey')
    
    mongoconnection = os.getenv('mongoconnection')
    mongoclient = os.getenv('mongodbclient')

    read_api = json.loads(check_api(football_api_key, data))
    json2 = str(json.dumps(read_api))

    now = datetime.now()
    date_time_now_format = now.strftime("%d/%m/%Y %H:%M:%S")

    # PROCESS ONLY DATA I WANT

    length = len(read_api['response'])
    fixtures_season = read_api['parameters']['season']
    fixtures_league = read_api['parameters']['league']

    
    

    for index in range(length):
        if (read_api["response"][index]["goals"]["home"] == None):
            fixture_goals_home = -1
        else:
            fixture_goals_home = read_api["response"][index]["goals"]["home"]

        if (read_api["response"][index]["goals"]["away"] == None):
            fixture_goals_away = -2
        else:
            fixture_goals_away = read_api["response"][index]["goals"]["away"]

        if (read_api["response"][index]["teams"]["home"]["winner"] == None):
            fixture_team_home_is_winner = 'NA'
        else:
            fixture_team_home_is_winner = read_api["response"][index]["teams"]["home"]["winner"]

        if (read_api["response"][index]["teams"]["away"]["winner"] == None):
            fixture_team_away_is_winner = 'NA'
        else:
            fixture_team_away_is_winner = read_api["response"][index]["teams"]["away"]["winner"]

        original_date = read_api["response"][index]["fixture"]["date"]
        date_time_obj = datetime.strptime(original_date, "%Y-%m-%dT%H:%M:%S%z")
        fixture_date_formatted = date_time_obj.strftime("%d/%m/%Y")
        fixture_time_formatted = date_time_obj.strftime("%H:%M:%S")
        fixture_id = read_api["response"][index]["fixture"]['id']
        fixture_date = fixture_date_formatted
        fixture_time = fixture_time_formatted
        fixture_status = read_api["response"][index]["fixture"]["status"]["long"]
        fixture_league_id = read_api["response"][index]["league"]["id"]
        fixture_season = read_api["response"][index]["league"]["season"]
        fixture_round = read_api['response'][index]['league']['round']
        fixture_team_home_id = read_api["response"][index]["teams"]["home"]["id"]
        fixture_team_away_id = read_api["response"][index]["teams"]["away"]["id"]
        fixture_team_home_name = read_api["response"][index]["teams"]["home"]["name"]
        fixture_team_away_name = read_api["response"][index]["teams"]["away"]["name"]

        if ((fixture_goals_home >= 0 and fixture_goals_away >= 0) and (fixture_team_home_is_winner == 'NA') and (fixture_goals_home == fixture_goals_away)):
            fixture_team_home_is_winner = 'DRAW'
            fixture_team_away_is_winner = 'DRAW'

        
        # https://stackoverflow.com/a/55080038
        fixutre_id_obj = object_id_from_int(fixture_id)
        
        json_i_want['FIXTURES'].append(
            # Example objectid:
            # 000000000000000000715870
            {   '_id': bson.objectid.ObjectId(fixutre_id_obj),
                'fixture_id': fixture_id,
                'fixture_utc_date': date_time_obj,
                'fixture_date': fixture_date,
                'fixture_time': fixture_time,
                # 'fixture_timezone': fixture_timezone,
                'fixture_status': fixture_status,
                'fixture_league_id': fixture_league_id,
                'fixture_season': fixture_season,
                'fixture_round': fixture_round,
                'fixture_team_home_id': fixture_team_home_id,
                'fixture_team_away_id': fixture_team_away_id,
                'fixture_team_home_name': fixture_team_home_name,
                'fixture_team_away_name': fixture_team_away_name,
                'fixture_goals_home': fixture_goals_home,
                'fixture_goals_away': fixture_goals_away,
                'fixture_team_home_is_winner': fixture_team_home_is_winner,
                'fixture_team_away_is_winner': fixture_team_away_is_winner,
            })

    return json_i_want['FIXTURES']
    # mongo_client = pymongo.MongoClient(mongoconnection)
    # latest_string = "latest__" + str(fixture_league_id)
    # mongo_db = mongo_client[mongoclient]
    # mongo_db = mongo_db.drop_collection(mongo_db[latest_string])
    # mongo_db = mongo_client[mongoclient]
    # mongo_collection = mongo_db[latest_string]

    # mongo_client = pymongo.MongoClient(mongoconnection)
    # # latest_string = "latest__" + str(fixture_league_id)
    # mongo_db = mongo_client[mongoclient]
    # mongo_collection = mongo_db["fixtures"]

    

def main(mytimer: func.TimerRequest):
    number_of_errors = 0
    blobcontainer = os.getenv('blobcontainer')
    blobconnection = os.getenv('blobconnection')
    mongoconnection = os.getenv('mongoconnection')
    mongoclient = os.getenv('mongodbclient')
    mongo_client = pymongo.MongoClient(mongoconnection)
    mongo_db = mongo_client[mongoclient]
    mongo_db = mongo_db.drop_collection("fixtures")
    mongo_db = mongo_client[mongoclient]
    mongo_collection = mongo_db["fixtures"]

    # json_i_want = {}
    # json_i_want["FIXTURES"] = []
    all_fixtures = {}
    all_fixtures["errors"] = []
    all_fixtures["info"] = []
    # all_fixtures["FIXTURES"] = []
    # all_fixtures["premier"] = []

    fixtures_premier_league = "/fixtures?league=41&season=2021"
    fixtures_leauge_one = "/fixtures?league=39&season=2021"

    all_fixtures["FIXTURES"] = process_api_data(fixtures_premier_league) + process_api_data(fixtures_leauge_one)

    # all_fixtures["league_one"] = []
    
    # all_fixtures["league_one"] = process_api_data(fixtures_leauge_one)

    # all_fixtures["FIXTURES"] =  all_fixtures["league_one"] +  all_fixtures["premier"]
    
    try:
        number_of_fixtures = 0
        number_of_fixtures = len(all_fixtures["FIXTURES"])
        mongo_collection.insert_many(all_fixtures["FIXTURES"])
        all_fixtures["info"].append({"numberOfFixtures": number_of_fixtures})
    except Exception as e:
        number_of_errors = number_of_errors + 1
        track = traceback.format_exc()
        all_fixtures["errors"].append({"errortype": str(e) + str(track), "numErrors": number_of_errors})
        all_fixtures["FIXTURES"] =  all_fixtures["league_one"] +  all_fixtures["premier"] + all_fixtures["errors"]



    parsed_json_i_want = json.dumps(all_fixtures, cls=DatetimeEncoder)

    now = datetime.now()
    now_str = now.strftime("%d-%m-%YT%H:%M:%S")
    output_filename = "bbet_fixtures__"+ now_str + "__.json"
    
    container_client = ContainerClient.from_connection_string(blobconnection, blobcontainer)
    blob_client = container_client.get_blob_client(output_filename)
    
    
    # TODO: try except this
    blob_client.upload_blob(parsed_json_i_want, overwrite=True)
