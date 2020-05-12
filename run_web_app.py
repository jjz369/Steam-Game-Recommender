from flask import Flask, render_template
import random
import yaml
from sqlalchemy import create_engine

app = Flask(__name__)


#path_steam_user_id = './data/steam_user_id.txt'


config = yaml.safe_load(open('./src/config.yaml'))
db_username = config['mysql']['username']
db_password = config['mysql']['password']
db_endpoint = config['mysql']['endpoint']
db_database = config['mysql']['database']
    
engine = create_engine('mysql+pymysql://{}:{}@{}/{}?charset=utf8mb4'.format(db_username, db_password, db_endpoint, db_database))


lst_user_id = [i[0] for i in engine.execute('select user_id from game_steam_user').fetchall()]



lst_popular_games = engine.execute('''
	SELECT 
		game_steam_app.app_id, 
		game_steam_app.name, 
		game_steam_app.initial_price, 
		game_steam_app.header_image 
	FROM game_steam_app
	JOIN recommended_games_popularity_based 
	ON game_steam_app.app_id = recommended_games_popularity_based.app_id
	AND game_steam_app.type = "game" 
	AND game_steam_app.release_date <= CURDATE() 
	AND game_steam_app.initial_price IS NOT NULL
	ORDER BY recommended_games_popularity_based.peak_today DESC 
	LIMIT 5''').fetchall()



@app.route('/')
def recommender():
	user_id = random.choice(lst_user_id)

	# user_id = 76561197960323774 # no purchase info

	lst_most_played_games = engine.execute('''
		SELECT 
			game_steam_app.app_id, 
			game_steam_app.name, 
			game_steam_app.initial_price, 
			game_steam_app.header_image 
		FROM game_steam_app
		JOIN game_steam_user
		ON game_steam_app.app_id = game_steam_user.app_id
		WHERE game_steam_user.user_id = {} 
		AND game_steam_user.playtime_forever > 0 
		AND game_steam_app.type = "game" 
		AND game_steam_app.release_date <= CURDATE() 
		AND game_steam_app.initial_price IS NOT NULL
		ORDER BY game_steam_user.playtime_forever DESC 
		LIMIT 3'''.format(user_id)).fetchall()



	if lst_most_played_games:
		favorite_app_id = lst_most_played_games[0][0]
		# get content based recommendation
		lst_content_recommended = engine.execute('''
			SELECT app_id, name, initial_price, header_image 
			FROM game_steam_app 
			WHERE type = "game" 
			AND release_date <= CURDATE() 
			AND initial_price IS NOT NULL
			AND app_id IN ({})'''.format(','.join(
				[str(i) for i in engine.execute('SELECT `0`,`1`,`2` FROM recommended_games_content_based WHERE app_id = {}'.format(favorite_app_id)).first()]
				)
			)
		).fetchall()


		# get item based recommendation
		lst_item_recommended = engine.execute('''
			SELECT app_id, name, initial_price, header_image 
			FROM game_steam_app 
			WHERE type = "game" 
			AND release_date <= CURDATE() 
			AND initial_price IS NOT NULL
			AND app_id IN ({})'''.format(','.join(
				[str(i) for i in engine.execute('SELECT `0`,`1`,`2` FROM recommended_games_item_based WHERE app_id = {}'.format(favorite_app_id)).first()]
				)
			)
		).fetchall()


		# get ALS based recommendation
		lst_als_recommended = engine.execute('''
			SELECT app_id, name, initial_price, header_image 
			FROM game_steam_app 
			WHERE type = "game" 
			AND release_date <= CURDATE() 
			AND initial_price IS NOT NULL
			AND app_id IN ({})'''.format(','.join(
				[str(i) for i in engine.execute('SELECT `0`,`1`,`2` FROM recommended_games_als_based WHERE user_id = {}'.format(user_id)).first()]
				)
			)
		).fetchall()

	else:
		lst_content_recommended = []
		lst_item_recommended = []
		lst_als_recommended = []




	return render_template('recommendation.html',
							user_id = user_id,
							lst_most_played_games = lst_most_played_games,
							lst_content_recommended = lst_content_recommended,
							lst_item_recommended = lst_item_recommended,
							lst_als_recommended = lst_als_recommended,
							lst_popular_games = lst_popular_games)


if __name__ == '__main__':
	app.run(debug=True)



