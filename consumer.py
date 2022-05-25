import os
import json
import time

import argparse

import yaml
import schedule

from redisqueue import RedisQueue

from pymongo import MongoClient


def db_connect(user, passwd, database, **kwargs):
    db_connection_str = f"mongodb+srv://{user}:{passwd}@cluster0.dojne.mongodb.net/{database}"

    conn = MongoClient(db_connection_str)

    return conn["final_project"]


def insert_data(conn):
    if len(batch_list) > 0:
        conn.insert_many(batch_list)
        print("insert the batch data")
        batch_list.clear()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()

    parser.add_argument("--time_interval", type=int, default=9)
    parser.add_argument("--collection", type=str, default="test")

    args = parser.parse_args()

    with open("awesome_link.yaml") as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)

    cloud_info = settings["cloud_info"]
    q = RedisQueue("final_project", host=cloud_info["host"], port=6379, db=0, decode_responses=True)
    db_profile = settings["db_profile"]

    conn = db_connect(**db_profile)
    conn_repo = conn["repository"]
    conn = conn[args.collection]

    global batch_list
    batch_list = []

    schedule.every(args.time_interval).minutes.do(insert_data, conn)

    print("Consumer Start!")

    while True:
        schedule.run_pending()
        msg = q.get(isBlocking=False)

        if msg is not None:
            msg = json.loads(msg)
            insert = msg["insert"]

            if "update" in msg:
                update_data = msg["update"]
                rid = update_data["rid"]
                uid = update_data["uid"]
                conn_repo.update_one({"rid": rid}, {"$push": {"star_user_list": uid}})

            if insert is not None:
                batch_list.append(msg)

            print(msg)
            print()

            time.sleep(3)



