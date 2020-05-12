#import run_web_api
from game_details import get_app_details, save_app_details
from user_owned_games import get_owned_games, save_owned_games
from recommendation import build_recommendation
import argparse
import os



if __name__ == "__main__":
    
    path_dir = os.path.dirname(os.path.realpath(__file__))
    os.chdir(path_dir)    
    
    parser = argparse.ArgumentParser()
    
    #parser.add_argument("x", type = int, help="the base")    
    #parser.add_argument("y", type=int, help="the exponent")
    
    # if want to scrape all steam game app details and information, use "-s" or "--scape"
    parser.add_argument("-sg", "--scapegames", action = "store_true", help = "get all steam game app details and informations")

   # if want to scrape the public shared user owned game details and information, use "-su" or "--scapeusers"
    parser.add_argument("-su", "--scapeusers", action = "store_true", help = "get public shared user owned game details and informations")
 
    
    # if want to build all games information table in the database separte from other options, use "-tg" or "--tablegames"
    parser.add_argument("-tg", "--tablegames", action =  "store_true", help = "build steam games information table in MySQL database")

   # if want to build all public shared user owned games  table in the database separte from other options, use "-tu" or "--tableusers"
    parser.add_argument("-tu", "--tableusers", action =  "store_true", help = "build public shared user owned games table in MySQL database")
 
    # build the recommendation similarity matrix and save to the database.
    parser.add_argument("-r", "--recommnedation", action =  "store_true", help = "build the recommendation system")

    
    # if want to build the steam game recommender from scratch, use "-a" or "--all"
    parser.add_argument("-a", "--all", action = "store_true", help = "build the steam game recommender from scratch.")
    
    
    args = parser.parse_args()
    if args.scapegames:
        get_app_details()
        
    if args.tablegames:
        save_app_details()
        
    if args.scapeusers:
        get_owned_games()
        
    if args.tableusers:
        save_owned_games()
        
    if args.recommnedation:
        build_recommendation()
        
    if args.all:
        save_app_details()
        save_owned_games()
        
        
    
    
#answer = args.x**args.y
#if args.verbosity >= 2:
#	print "Running '{}'".format(__file__)
#if args.verbosity >= 1:
#	print "{}^{} == ".format(args.x, args.y)
#print answer