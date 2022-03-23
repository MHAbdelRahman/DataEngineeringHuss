import json

from pymongo import MongoClient


class DBConnection:
    def __init__(self):
        with open("../db_info/credentials.json") as f:
            cred = json.load(f)
            username = cred['user']
            password = cred['password']
        conn_string = f'mongodb+srv://{username}:{password}@aui-de-assignments.cohiy.mongodb.net'
        self.__db = MongoClient(conn_string).football_assignment

    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = super(DBConnection, cls).__new__(cls)
        return cls.instance

    def get_users_info(self):
        pipeline = [
            {
                "$lookup":
                    {
                        "from": "clubs",
                        "localField": "club",
                        "foreignField": "Club",
                        "as": "club_info"
                    }
            },
            {
                "$project":
                    {
                        "_id": 0,
                        "external_id_str": 1,
                        "name": 1,
                        "club": 1,
                        "club_info":
                            {
                                "Country": 1,
                                "UCL": 1,
                                "UEL": 1,
                            }
                    }
            }
        ]
        unprocessed_list = self.__db.users.aggregate(pipeline)
        processed_list = list()
        for res in unprocessed_list:
            res["Country"] = res["club_info"][0]["Country"]
            res["total_wins_uefa"] = res["club_info"][0]["UCL"] + res["club_info"][0]["UEL"]
            res.pop("club_info")
            processed_list.append(res)

        return processed_list
