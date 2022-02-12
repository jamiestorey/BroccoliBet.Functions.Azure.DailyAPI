# import datetime
# import logging

# import azure.functions as func


# def main(mytimer: func.TimerRequest) -> None:
#     utc_timestamp = datetime.datetime.utcnow().replace(
#         tzinfo=datetime.timezone.utc).isoformat()

#     if mytimer.past_due:
#         logging.info('The timer is past due!')

#     logging.info('Python timer trigger function ran at %s', utc_timestamp)

import logging
import os
import http.client
import json
from datetime import datetime
import mysql.connector
import pathlib
from azure.storage.blob import ContainerClient
import azure.functions as func
import pymongo


# https://stackoverflow.com/questions/11875770/how-to-overcome-datetime-datetime-not-json-serializable/
class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)


def get_ssl_cert():
    current_path = pathlib.Path(__file__).parent.parent
    print(current_path)
    return str(current_path / 'DigiCertGlobalRootCA.crt.pem')



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

#-> None


def process_api_data(data):
    # thepath = get_ssl_cert()
    football_api_key = os.getenv('footballapikey')
    # mysql_azure_host = os.getenv('mysqlazurehost')
    # mysql_azure_user = os.getenv('mysqlazureuser')
    # mysql_azure_password = os.getenv('mysqlazurepassword')
    # mysql_azure_db = os.getenv('mysqlazuredb')
    blobcontainer = os.getenv('blobcontainer')
    blobconnection = os.getenv('blobconnection')
    mongoconnection = os.getenv('mongoconnection')
    mongoclient = os.getenv('mongodbclient')

    jsondata = json.loads(check_api(football_api_key, data))
    json2 = str(json.dumps(jsondata))

    # mydb = mysql.connector.connect(
    #     host=mysql_azure_host,
    #     user=mysql_azure_user,
    #     password=mysql_azure_password,
    #     database=mysql_azure_db,
    #     port=3306,
    #     ssl_verify_cert=True,
    #     ssl_ca=thepath,
    #     use_pure=True
    # )

    # mycursor = mydb.cursor()

    now = datetime.now()
    date_time_now_format = now.strftime("%d/%m/%Y %H:%M:%S")

    # todo - remove sql stuff
    # val = (data, date_time_now_format, json2)
    # sql = "insert into my_table( item_name, item_description, example) VALUES (%s, %s, %s)"
    # mycursor.execute(sql, val)


    # PROCESS ONLY DATA I WANT
    read_api = jsondata

    length = len(read_api['response'])
    fixtures_season = read_api['parameters']['season']
    fixtures_league = read_api['parameters']['league']

    json_i_want = {"name":"placeholder", "date": date_time_now_format}
    json_i_want['FIXTURES'] = []

    for index in range(length):
        if (read_api["response"][index]["goals"]["home"] == None):
            fixture_goals_home = -111
        else:
            fixture_goals_home = read_api["response"][index]["goals"]["home"]

        if (read_api["response"][index]["goals"]["away"] == None):
            fixture_goals_away = -222
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
        # fixture_timezone = read_api['response'][index]['fixture']['timezone']
        fixture_status = read_api["response"][index]["fixture"]["status"]["long"]
        fixture_league_id = read_api["response"][index]["league"]["id"]
        fixture_round = read_api['response'][index]['league']['round']
        fixture_team_home_id = read_api["response"][index]["teams"]["home"]["id"]
        fixture_team_away_id = read_api["response"][index]["teams"]["away"]["id"]
        fixture_team_home_name = read_api["response"][index]["teams"]["home"]["name"]
        fixture_team_away_name = read_api["response"][index]["teams"]["away"]["name"]
        # fixture_team_home_is_winner = read_api["response"][index]["teams"]["home"]["winner"]
        # fixture_team_away_is_winner = read_api["response"][index]["teams"]["away"]["winner"]

        # fixture_goals_home = read_api["response"][index]["goals"]["home"]
        # fixture_goals_away = read_api["response"][index]["goals"]["away"]
        # f_score_ht_home = read_api["response"][index]["score"]['halftime']["home"]
        # f_score_ht_away = read_api["response"][index]["score"]['halftime']["away"]
        # f_score_ft_home = read_api["response"][index]["score"]['fulltime']["home"]
        # f_score_ft_away = read_api["response"][index]["score"]['fulltime']["away"]
        # f_score_et_home = read_api["response"][index]["score"]['extratime']["home"]
        # f_score_et_away = read_api["response"][index]["score"]['extratime']["away"]
        # f_score_pen_home = read_api["response"][index]["score"]['penalty']["home"]
        # f_score_pen_away = read_api["response"][index]["score"]['penalty']["away"]

        if ((fixture_goals_home >= 0 and fixture_goals_away >= 0) and (fixture_team_home_is_winner == 'NA') and (fixture_goals_home == fixture_goals_away)):
            fixture_team_home_is_winner = 'DRAW'
            fixture_team_away_is_winner = 'DRAW'

        json_i_want['FIXTURES'].append(
            {   '_id': fixture_id,
                'fixture_id': fixture_id,
                'fixture_utc_date': date_time_obj,
                'fixture_date': fixture_date,
                'fixture_time': fixture_time,
                # 'fixture_timezone': fixture_timezone,
                'fixture_status': fixture_status,
                'fixture_league_id': fixture_league_id,
                'fixture_round': fixture_round,
                'fixture_team_home_id': fixture_team_home_id,
                'fixture_team_away_id': fixture_team_away_id,
                'fixture_team_home_name': fixture_team_home_name,
                'fixture_team_away_name': fixture_team_away_name,
                'fixture_goals_home': fixture_goals_home,
                'fixture_goals_away': fixture_goals_away,
                'fixture_team_home_is_winner': fixture_team_home_is_winner,
                'fixture_team_away_is_winner': fixture_team_away_is_winner,
                # 'f_score_ht_home': f_score_ht_home,
                # 'f_score_ht_away': f_score_ht_away,
                # 'f_score_ft_home': f_score_ft_home,
                # 'f_score_ft_away': f_score_ft_away,
                # 'f_score_et_home': f_score_et_home,
                # 'f_score_et_away': f_score_et_away,
                # 'f_score_pen_home': f_score_pen_home,
                # 'f_score_pen_away': f_score_pen_away,
            })

    # parsed_json_i_want = json.dumps(json_i_want)
    parsed_json_i_want = json.dumps(json_i_want, cls=DatetimeEncoder)

    output_filename = "latest__" + str(fixture_league_id) + ".json"
    # output_folder = get_folder(output_filename)
    # with open(output_folder , 'w') as writeJson:
    #     json.dump(json_i_want, writeJson, indent=4)

    
    container_client = ContainerClient.from_connection_string(blobconnection, blobcontainer)
    blob_client = container_client.get_blob_client(output_filename)
    
    blob_client.upload_blob(parsed_json_i_want, overwrite=True)
    
    # mongo_client = pymongo.MongoClient(mongoconnection)
    # mongo_db = mongo_client["sportsapi"]
    # latest_string = "latest__" + str(fixture_league_id)
    # mongo_collection = mongo_db[latest_string]

    mongo_client = pymongo.MongoClient(mongoconnection)
    latest_string = "latest__" + str(fixture_league_id)
    mongo_db = mongo_client[mongoclient]
    mongo_db = mongo_db.drop_collection(mongo_db[latest_string])
    mongo_db = mongo_client[mongoclient]
    mongo_collection = mongo_db[latest_string]

    mongo_collection.insert_many(json_i_want['FIXTURES'])
    
    #mongo_data_test = parsed_json_i_want
    # mongo_collection.replace_one({"name":"placeholder"}, json_i_want)
    # now = datetime.now()
    # date_time_now_format = now.strftime("%d/%m/%Y %H:%M:%S")
    
    #todo remove sql stuff
    # sql = "insert into my_table( item_name, item_description, example) VALUES (%s, %s, %s)"
    # val = ('PARSED '+ fixtures_league + data, date_time_now_format, parsed_json_i_want)
    # mycursor.execute(sql, val)
    # mycursor.close()
    # mydb.commit()
    # mydb.close()

    #can't delete files on functions server
    # if os.path.exists(get_folder(output_filename)):
    #     os.remove(get_folder(output_filename))

def main(mytimer: func.TimerRequest):
    #utc_timestamp = datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    GETTT = "/fixtures?league=41&season=2021"
    # GETTT = "/fixtures?league=41&season=2021&timezone=Europe/london"
    process_api_data(GETTT)

    GETTT = "/fixtures?league=39&season=2021"

    process_api_data(GETTT)

    #return f'Number of fixtures --> [{length}]'

