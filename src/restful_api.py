from math import ceil

from flask import Flask, request
from flask_restful import Api, Resource

from Utils import constants
from Utils.utils import Utils


# Action: Get /info
class Info(Resource):
    def get(self):
        users_list = Utils.get_users_info()
        page = request.args.get('page', default=0, type=int)
        per_page = request.args.get('per_page', default=10, type=int)
        number_of_pages = ceil(len(users_list) / per_page)

        if page < 0 or page > number_of_pages:
            page = 0
        if per_page < 1:
            per_page = 1
        elif per_page > len(users_list):
            per_page = len(users_list)

        if page == 0:
            current_page_res = users_list
        else:
            starting_point = (page - 1) * per_page
            end_point = starting_point + per_page
            if end_point > len(users_list):
                end_point = len(users_list)
            current_page_res = users_list[starting_point:end_point]

        info = {
            "page": page,
            "per_page": per_page,
            "number of pages": number_of_pages
        }
        return {"info": info, constants.JSON_RESULTS_NAME: current_page_res}


if __name__ == '__main__':
    app = Flask(__name__)
    api = Api(app)
    api.add_resource(Info, "/info")
    app.run(
        host="0.0.0.0",
        port=8888
    )
