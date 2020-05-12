import requests
import time
import json
import threading
import queue
import yaml
from sqlalchemy import create_engine
from sqlalchemy.types import BigInteger, Integer
import pandas as pd

def split_list(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def worker_get_owned_games(lst_user_id, api_key, q):
    dic_temp = {}
    for user_id in lst_user_id:
        for i in range(3):
            try:
                r = requests.get(
                    url = 'http://api.steampowered.com/IPlayerService/GetOwnedGames/v0001/', 
                    params = {
                        'key' : api_key,
                        'steamid' : user_id,
                        'include_played_free_games': True,
                        'format' : 'json' 
                    }
                )
                dic_owned_games = r.json().get('response').get('games')
                dic_temp[user_id] = dic_owned_games
                time.sleep(.5)
                break
            except Exception as e:
                print(user_id, e)
                time.sleep(5) 

    q.put(dic_temp)

def get_owned_games():
    
    config = yaml.safe_load(open('config.yaml'))
    api_key = config['steam']['api_key']
    if not api_key:
        return "No API Key found!"

    dic_owned_games = {}

    with open('../data/steam_user_id.txt', 'r') as f:
        lst_user_id = [i.strip() for i in f.readlines()]

    print("The number of user ids: {}".format(len(lst_user_id)))
    for lst_user_id_chunk in list(split_list(lst_user_id, 500)):
        lst_thread = []
        q = queue.Queue()

        for i in list(split_list(lst_user_id_chunk, 100)):
            t = threading.Thread(target = worker_get_owned_games, args = (i, api_key, q,))
            lst_thread.append(t)

        for i in lst_thread:
            i.start()
        
        for i in lst_thread:
            i.join()

        while not q.empty():
            dic_owned_games.update(q.get())

    with open('../data/steam_owned_games.txt', 'w') as f:
        for k,v in dic_owned_games.items():
            f.write(json.dumps({k:v}))
            f.write('\n')



def save_owned_games():
    
    config = yaml.safe_load(open('config.yaml'))
    db_username = config['mysql']['username']
    db_password = config['mysql']['password']
    db_endpoint = config['mysql']['endpoint']
    db_database = config['mysql']['database']
    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(db_username, db_password, db_endpoint, db_database))

    
    dic_owned_games = {}
    with open('../data/steam_owned_games.txt', 'r') as f:
        for raw_string in f.readlines():
            user_id, lst_inventory = list(json.loads(raw_string).items())[0]
            if lst_inventory:
                for i in lst_inventory:
                    app_id = i.get('appid')
                    playtime_forever = i.get('playtime_forever', 0)
                    if playtime_forever > 0:
                        dic_owned_games.update({
                            (user_id, app_id) : {
                                'user_id' : user_id,
                                'app_id' : app_id,
                                'playtime_forever' : playtime_forever
                            }
                        })
    df_owned_games = pd.DataFrame.from_dict(dic_owned_games, 'index')
    df_owned_games.to_sql(
        'game_steam_user', 
        engine, 
        if_exists='replace', 
        index=False, 
        dtype={
            'user_id': BigInteger(),
            'app_id': Integer(),
            'playtime_forever': Integer()
        }, 
        chunksize = 10000
    )


save_owned_games()







