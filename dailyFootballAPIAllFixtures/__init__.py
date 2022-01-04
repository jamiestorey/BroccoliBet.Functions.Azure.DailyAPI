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

import azure.functions as func


def get_ssl_cert():
    current_path = pathlib.Path(__file__).parent.parent
    print(current_path)
    return str(current_path / 'DigiCertGlobalRootCA.crt.pem')


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
    thepath = get_ssl_cert()
    user_name = os.getenv('UsernameFromKeyVault')
    password = os.getenv('PasswordFromKeyVault')
    football_api_key = os.getenv('football-api-key')
    mysql_azure_host = os.getenv('mysql_azure_host')
    mysql_azure_user = os.getenv('mysql_azure_user')
    mysql_azure_password = os.getenv('mysql_azure_password')
    mysql_azure_db = os.getenv('mysql_azure_db')
    
    jsondata = json.loads(check_api(football_api_key, data))
    json2 = str(json.dumps(jsondata))

    mydb = mysql.connector.connect(
        host=mysql_azure_host,
        user=mysql_azure_user,
        password=mysql_azure_password,
        database=mysql_azure_db,
        port=3306,
        ssl_verify_cert=True,
        ssl_ca=thepath,
        use_pure=True
    )

    mycursor = mydb.cursor()

    sql = "insert into my_table( item_name, item_description, example) VALUES (%s, %s, %s)"
    now = datetime.now()
    date_time_now_format = now.strftime("%d/%m/%Y %H:%M:%S")
    val = (data, date_time_now_format, json2)
    mycursor.execute(sql, val)


    # PROCESS ONLY DATA I WANT
    read_api = jsondata

    length = len(read_api['response'])
    fixtures_season = read_api['parameters']['season']
    fixtures_league = read_api['parameters']['league']

    json_i_want = {}
    json_i_want['fixtures'] = []

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
        # print(f"original_date = {original_date}")
        date_time_obj = datetime.strptime(original_date, "%Y-%m-%dT%H:%M:%S%z")
        fixture_date_formatted = date_time_obj.strftime("%d/%m/%Y")
        fixture_time_formatted = date_time_obj.strftime("%H:%M:%S")

        fixture_id = read_api["response"][index]["fixture"]['id']
        fixture_date = fixture_date_formatted
        fixture_time = fixture_time_formatted
        # fixture_timezone = read_api['response'][index]['fixture']['timezone']
        fixture_status = read_api["response"][index]["fixture"]["status"]["long"]
        fixture_league_id = read_api["response"][index]["league"]["id"]
        fixtures_round = read_api['response'][index]['league']['round']
        fixture_team_home_id = read_api["response"][index]["teams"]["home"]["id"]
        fixture_team_away_id = read_api["response"][index]["teams"]["away"]["id"]
        fixture_team_home_name = read_api["response"][index]["teams"]["home"]["name"]
        fixture_team_away_name = read_api["response"][index]["teams"]["away"]["name"]
        # fixture_team_home_is_winner = read_api["response"][index]["teams"]["home"]["winner"]
        # fixture_team_away_is_winner = read_api["response"][index]["teams"]["away"]["winner"]

        # fixture_goals_home = read_api["response"][index]["goals"]["home"]
        # fixture_goals_home = is_home_goals_null
        # fixture_goals_away = read_api["response"][index]["goals"]["away"]
        # fixture_goals_away = is_away_goals_null
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

        json_i_want['fixtures'].append({
            'fixture_id': fixture_id,
            'fixture_date': fixture_date,
            'fixture_time': fixture_time,
            # 'fixture_timezone': fixture_timezone,
            'fixture_status': fixture_status,
            'fixture_league_id': fixture_league_id,
            'fixtures_round': fixtures_round,
            'fixture_team_home_id': fixture_team_home_id,
            'fixture_team_away_id': fixture_team_away_id,
            # 'fixture_team_home_name': fixture_team_home_name,
            # 'fixture_team_away_name': fixture_team_away_name,
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

    parsed_json_i_want = json.dumps(json_i_want)
    sql = "insert into my_table( item_name, item_description, example) VALUES (%s, %s, %s)"
    now = datetime.now()
    date_time_now_format = now.strftime("%d/%m/%Y %H:%M:%S")
    val = ('PARSED '+ fixtures_league + data, date_time_now_format, parsed_json_i_want)
    mycursor.execute(sql, val)
    mycursor.close()
    mydb.commit()
    mydb.close()

def main(mytimer: func.TimerRequest):
    #utc_timestamp = datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    GETTT = "/fixtures?league=41&season=2021&timezone=Europe/london"
    process_api_data(GETTT)

    GETTT = "/fixtures?league=39&season=2021&timezone=Europe/london"

    process_api_data(GETTT)

    #return f'Number of fixtures --> [{length}]'

