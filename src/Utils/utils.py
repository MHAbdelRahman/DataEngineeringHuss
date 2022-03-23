from src.db_connection import DBConnection


class Utils:
    users_list = None

    def __init__(self):
        pass

    @staticmethod
    def get_users_info(force_db_call=False):
        if Utils.users_list is None:
            Utils.users_list = DBConnection().get_users_info()
        return Utils.users_list
