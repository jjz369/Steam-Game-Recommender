import requests, re, os
import pandas as pd
import numpy as np
import yaml

from bs4 import BeautifulSoup
from sqlalchemy import create_engine

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from pyspark.ml.recommendation import ALS
from pyspark import SparkContext
from pyspark.sql import SparkSession


#####################################
##### Model 1: Popularity Based #####
#####################################


def recommendation_popularity_based(engine):
    url = 'https://store.steampowered.com/stats'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')

    dic_current_player = {}

    for i in soup.find('div', {'id':'detailStats'}).find_all('tr', {'class':'player_count_row'}):
        lst_data = i.find_all('td')
        current_player = int(lst_data[0].span.string.replace(',',''))
        peak_today = int(lst_data[1].span.string.replace(',',''))
        app_id = re.findall(r'(\d+)', lst_data[-1].a.get('href'))[0]
        dic_current_player[app_id] = {'current_player' : current_player, 'peak_today' : peak_today}

    df_popularity_based_result = pd.DataFrame.from_dict(dic_current_player, 'index')
    df_popularity_based_result.index.name = 'app_id'
    df_popularity_based_result.reset_index(inplace=True)
    df_popularity_based_result.to_sql('recommended_games_popularity_based', engine, if_exists='replace', index = False)



#####################################
##### Model 2: Content Based #####
#####################################

def recommendation_content_based(engine):

    df_game_description = pd.read_sql_query(
        '''
            SELECT 
                app_id, 
                short_description 
            FROM game_steam_app
            WHERE short_description IS NOT NULL
            AND type = "game" 
            AND name IS NOT NULL
            AND release_date <= CURDATE() 
            AND initial_price IS NOT NULL
        ''', engine)

    tfidf = TfidfVectorizer(strip_accents='unicode',stop_words='english').fit_transform(df_game_description['short_description'].tolist())

    lst_app_id = df_game_description['app_id'].tolist()
    dic_recomended = {}
    for row_index in range(tfidf.shape[0]):
        cosine_similarities = linear_kernel(tfidf[row_index:row_index+1], tfidf).flatten()
        top_related_rows = cosine_similarities.argsort()[-2:-22:-1]
        dic_recomended.update({lst_app_id[row_index]:[lst_app_id[i] for i in top_related_rows]})


    df_content_based_results = pd.DataFrame.from_dict(dic_recomended, 'index')
    df_content_based_results.index.name = 'app_id'
    df_content_based_results.reset_index(inplace=True)
    df_content_based_results.to_sql('recommended_games_content_based',engine,if_exists='replace', index = False)



# Model 3: item based
def recommendation_item_based(engine):

    df_purchase = pd.read_sql_query(
        '''
        SELECT app_id, user_id         
        FROM game_steam_user
        WHERE playtime_forever > 15
        ''', engine).pivot_table(values = 'user_id', index = ['app_id'], columns = ['user_id'], aggfunc = len, fill_value = 0)

    purchase_matrix = df_purchase.values
    lst_app_id = df_purchase.index

    dic_recomended_item_based = {}
    for index in range(purchase_matrix.shape[0]):
        cosine_similarities = linear_kernel(purchase_matrix[index:index+1], purchase_matrix).flatten()
        lst_related_app = np.argsort(-cosine_similarities)[1:101]
        dic_recomended_item_based.update({lst_app_id[index]:[lst_app_id[i] for i in lst_related_app]})

    df_item_based_result = pd.DataFrame.from_dict(dic_recomended_item_based, 'index')
    df_item_based_result.index.name = 'app_id'
    df_item_based_result.reset_index(inplace=True)
    df_item_based_result.to_sql('recommended_games_item_based', engine, if_exists='replace', chunksize = 1000, index = False)



# Model 4: Collaborative Filtering

def recommendation_als_based(engine):

    config = yaml.safe_load(open('{}/config.yaml'.format(os.path.dirname(os.path.realpath(__file__)))))
    db_username = config['mysql']['username']
    db_password = config['mysql']['password']
    db_endpoint = config['mysql']['endpoint']
    db_database = config['mysql']['database']

    sc=SparkContext()
    spark = SparkSession(sc)

    # If haveing problem wit drivers: move your MySQL JDBC driver to the jars folder of pyspark
    # Ref: https://stackoverflow.com/questions/49011012/cant-connect-to-mysql-database-from-pyspark-getting-jdbc-error

    spark.read.format("jdbc").option("url", "jdbc:mysql://{}/{}".format(db_endpoint, db_database))\
                .option("user", db_username).option("password", db_password)\
                .option("dbtable", "game_steam_user")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .load().createOrReplaceTempView('user_inventory')



    spark.read.format("jdbc").option("url", "jdbc:mysql://{}/{}".format(db_endpoint, db_database))\
                .option("user", db_username).option("password", db_password)\
                .option("dbtable", "game_steam_app")\
                .option("driver", "com.mysql.cj.jdbc.Driver")\
                .load().createOrReplaceTempView('game_steam_app')
            
    df_user_playtime = spark.sql('''
        SELECT 
            DENSE_RANK() OVER (ORDER BY user_id) AS user, 
            user_id, 
            app_id AS item, 
            LOG(playtime_forever) AS rating 
        FROM user_inventory
        WHERE playtime_forever >= 5
    ''')

    df_valid_games = spark.sql('''
        SELECT app_id 
        FROM game_steam_app 
        WHERE short_description IS NOT NULL 
        AND name IS NOT NULL 
        AND type = "game" 
        AND initial_price IS NOT NULL
    ''')
    df_user_inventory = df_user_playtime.join(df_valid_games, df_user_playtime['item'] == df_valid_games['app_id'], 'inner').select('user','user_id','item','rating')

    dic_real_user_id = df_user_inventory.select('user','user_id').toPandas().set_index('user')['user_id'].to_dict()
    als = ALS(rank = 10)
    model = als.fit(df_user_inventory)
    recommended_games = model.recommendForAllUsers(10)
    dic_recomended_als_based = {}
    for user, lst_recommended_games in recommended_games.select('user', 'recommendations.item').toPandas().set_index('user')['item'].to_dict().items():
        user_id = dic_real_user_id.get(user)
        dic_recomended_als_based[user_id] = {}
        for i, app_id in enumerate(lst_recommended_games):
            dic_recomended_als_based[user_id].update({i:app_id})


    df_als_based_result = pd.DataFrame.from_dict(dic_recomended_als_based, 'index')
    df_als_based_result.index.name = 'user_id'
    df_als_based_result.reset_index(inplace=True)
    df_als_based_result.to_sql('recommended_games_als_based', engine, if_exists='replace', chunksize = 1000, index = False)


def build_recommendation():

    config = yaml.safe_load(open('config.yaml'))
    db_username = config['mysql']['username']
    db_password = config['mysql']['password']
    db_endpoint = config['mysql']['endpoint']
    db_database = config['mysql']['database']

    engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(db_username, db_password, db_endpoint, db_database))

    recommendation_popularity_based(engine)
    recommendation_content_based(engine)
    recommendation_item_based(engine)
    recommendation_als_based(engine)


