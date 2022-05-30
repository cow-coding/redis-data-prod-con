import json
import os, sys

sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import re
import time
from datetime import datetime

import yaml
import argparse
from distutils.util import strtobool

import requests
from dotenv import load_dotenv

from pymongo import MongoClient

from redisqueue import RedisQueue


def db_connect(user, passwd, database, **kwargs):
    db_connection_str = f"mongodb+srv://{user}:{passwd}@cluster0.dojne.mongodb.net/{database}"

    conn = MongoClient(db_connection_str)

    return conn["final_project"]


def get_user_star_count(login, headers):
    url = f"https://api.github.com/users/{login}/starred?per_page=1"
    res = requests.get(url, headers=headers)
    res.raise_for_status()

    head = res.headers

    if "Link" in head:
        stars = int(head["Link"].split(",")[-1].split(";")[0].strip().lstrip("<").rstrip(">").split("=")[-1])
        return stars
    else:
        return 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    load_dotenv(verbose=True)

    parser.add_argument("--token_idx", type=int, default=0)

    args = parser.parse_args()

    with open(os.getenv("SETTINGS")) as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)

    token_list = settings["token"]
    token_idx = args.token_idx
    token = token_list[token_idx]
    token_count = 1
    data_profile = settings["data_profile"]
    db_profile = settings["db_profile"]
    cloud_info = settings["cloud_info"]
    headers = {"Authorization": "token " + token,
               "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_5) AppleWebKit/537.36 (KHTML, like Gecko) "
                             "Chrome/84.0.4147.89 Safari/537.36"}

    q = RedisQueue("final_project", host=cloud_info["host"], port=6379, db=0, decode_responses=True)

    print("Get User Data...")
    db = db_connect(**db_profile)
    collection = db.get_collection("user_info")
    user_list = list(collection.find({"stars": {"$exists": False}}))

    data_dict = dict()
    insert_dict = dict()
    update_dict = dict()

    user_idx = 0

    while user_idx < len(user_list):
        login = user_list[user_idx]["login"]
        uid = user_list[user_idx]["uid"]

        try:
            stars = get_user_star_count(login, headers)

            update_dict["condition"] = {"uid": uid}
            update_dict["query"] = {
                "$set": {
                    "stars": stars
                }
            }

            data_dict["insert"] = insert_dict
            data_dict["update"] = update_dict

            insert_data = json.dumps(data_dict)
            q.put(insert_data)
            print(f"[{user_idx:>8}|{len(user_list)}] ({user_idx/len(user_list)*100}%)")
            print(f"{insert_data} - insert time: {datetime.now()}")
            print()
        except requests.exceptions.HTTPError as httperr:
            if httperr.response.status_code == 403:
                if token_count >= len(token_list):
                    print("Wait token reset")
                    time.sleep(60 * 60)
                    token_count = 1
                else:
                    token_count += 1
                token_idx += 1
                token_idx = token_idx % len(token_list)
                token = token_list[token_idx]
                headers["Authorization"] = "token " + token
            elif httperr.response.status_code == 404:
                user_idx += 1
