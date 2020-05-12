import requests
import time
import json
import yaml
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.types import Integer


def get_app_details():
    # The Steam API limits 200 data in 5 miniutes time. So after every 200 data, sleep for 5 minitues.
    Ns = 38400
    current_count = Ns
    
    url = 'http://api.steampowered.com/ISteamApps/GetAppList/v2'
    r = requests.get(url)
    dic_steam_app = r.json()
    lst_app_id = [i.get('appid') for i in dic_steam_app.get('applist').get('apps')]
    with open('../data/steam_app_details_2.txt', 'w') as f:
        for app_id in sorted(lst_app_id)[Ns:]:
            for i in range(3):
                try:
                    r = requests.get(
                        url = 'http://store.steampowered.com/api/appdetails/', 
                        params = { 'appids' : app_id }
                    )
                    dic_app_data = r.json()
                    break
                except Exception as e:
                    print(app_id, e)
                    time.sleep(.5)
                    
            f.write(json.dumps(dic_app_data))
            f.write('\n')

            if current_count > Ns and current_count % 200 == 0:
                print("The number of games: {}, current id: {}".format(len(lst_app_id), current_count))
                time.sleep(300)
            current_count += 1


def save_app_details():
    dic_app_details = {}

    config = yaml.safe_load(open('config.yaml'))
    db_username = config['mysql']['username']
    db_password = config['mysql']['password']
    db_endpoint = config['mysql']['endpoint']
    db_database = config['mysql']['database']
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(db_username, db_password, db_endpoint, db_database))
	
    with open('../data/steam_app_details.txt', 'r') as f:
        for i in f.readlines():
            try:
                for app_id, dic_response in json.loads(i).items():
                    if dic_response.get('success'):
                        dic_app_details[app_id] = parse_steam_app_details(dic_response.get('data',{}))
            except:
                pass
    df_steam_app = pd.DataFrame.from_dict(dic_app_details, 'index')
    df_steam_app.index.name = 'app_id'
    df_steam_app.reset_index(inplace=True)
    df_steam_app.to_sql('game_steam_app', engine, if_exists='replace', index=False, chunksize = 10000, dtype={'app_id':Integer(), 'required_age':Integer()})

    


def parse_steam_app_details(app_data):
    developers = ', '.join(app_data.get('developers', []))
    if not developers:
        developers = None
    publishers = ', '.join(app_data.get('publishers', []))
    if not publishers:
        publishers = None
    name = app_data.get('name')
    required_age = app_data.get('required_age')
    short_description = app_data.get('short_description')
    if not short_description:
        short_description = None
    app_type = app_data.get('type')
    header_image = app_data.get('header_image')
    fullgame = app_data.get('fullgame',{}).get('appid')
    lst_categories = app_data.get('categories',[])
    if lst_categories:
        categories = ', '.join([i.get('description') for i in lst_categories])
    else:
        categories = None
    lst_genres = app_data.get('genres',[])
    if lst_genres:
        genres = ', '.join([i.get('description') for i in lst_genres])
    else:
        genres = None
    supported_languages = app_data.get('supported_languages')
    if supported_languages:
        supported_languages = supported_languages.replace('<strong>*</strong>', '').replace('<br>languages with full audio support','')
    if app_data.get('is_free') == True:
        initial_price = 0
        currency = 'USD'
    else:
        if app_data.get('price_overview',{}):
            initial_price = app_data.get('price_overview',{}).get('initial', 0) / 100
            currency = app_data.get('price_overview',{}).get('currency')
        else:
            initial_price = None
            currency = None

    if app_data.get('release_date',{}).get('coming_soon') == False:
        release_date = app_data.get('release_date',{}).get('date')
        if release_date:
            try:
                release_date = datetime.strptime(release_date, '%b %d, %Y').date()
            except Exception as e:
                try:
                    release_date = datetime.strptime(release_date, '%d %b, %Y').date()
                except:
                    try:
                        release_date = datetime.strptime(release_date, '%b %Y').date()
                    except:
                        release_date = None
        else:
            release_date = None
    else:
        release_date = None

    dic_steam_app = {
        'name' : name,
        'type' : app_type,
        'release_date' : release_date,
        'currency' : currency,
        'initial_price' : initial_price,
        'short_description' : short_description,
        'header_image' : header_image,
        'fullgame' : fullgame,
        'developers' : developers,
        'publishers' : publishers,
        'required_age' : required_age,
        'supported_languages' : supported_languages,
        'categories' : categories,
        'genres' : genres,
    }

    return dic_steam_app
