import os
import json
import time

import yaml
import schedule

from redisqueue import RedisQueue

from pymongo import MongoClient


def db_connect(user, passwd, database, **kwargs):
    db_connection_str = f"mongodb+srv://{user}:{passwd}@cluster0.dojne.mongodb.net/{database}"

    conn = MongoClient(db_connection_str)

    return conn["final_project"]


def insert_data(batch_data, conn):
    if len(batch_data) > 0:
        conn.insert_many(batch_data)
        print("insert the batch data")


if __name__ == '__main__':
    with open("awesome_link.yaml") as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)

    q = RedisQueue("final_project", host="35.216.103.242", port=6379, db=0, decode_responses=True)
    db_profile = settings["db_profile"]

    conn = db_connect(**db_profile)
    conn = conn["test"]

    BATCH_SIZE = 15
    batch_list = []

    schedule.every(15).minute.do(insert_data, batch_list, conn)

    while True:
        msg = q.get(isBlocking=True)

        if msg is not None:
            msg = json.loads(msg)

            print(msg)
            print()

            batch_list.append(msg)
            time.sleep(2)




