import os


class Config:

    GROUP_NAME = 'ubank_test'
    PROJECT_NAME = 'transfer_database_direct'

    SCHEDULED_DICT = {
        'time_interval': int(os.getenv('TIME_INTERVAL', 1440)),             # 定时时间间隔24小时
    }

    BASE_DIR = os.path.dirname(os.path.dirname(__file__))

    ROOT_DIR = os.path.dirname(BASE_DIR)
    DATA_DIR = os.path.join(ROOT_DIR, 'dataout')
    os.makedirs(DATA_DIR, exist_ok=True)

    LOG_DIR = os.path.join(BASE_DIR, 'logs')
    os.makedirs(LOG_DIR, exist_ok=True)

    TIMEZONE = 'Asia/Shanghai'

    HOST_LOCAL = '192.168.3.250'
    # HOST_REMOTE = '122.114.50.172'
    HOST_REMOTE = '192.168.3.250'

    MONGO_DICT = {
        'host': HOST_LOCAL,
        'port': 27017,
        'db': GROUP_NAME,
        'username': 'root',
        'password': '123456',
    }

    MONGO_DICT_REMOTE = {
        'host': HOST_REMOTE,
        'port': 27017,
        'db': GROUP_NAME,
        'username': 'myuser',
        'password': '123456',
    }

    MYSQL_DICT_REMOTE = {
        'host': HOST_REMOTE,
        'port': 3306,
        'db': 'ubank',
        'user': 'root',
        'password': '123456',
    }
