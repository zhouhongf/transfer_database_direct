from pymongo import MongoClient, collection
from config import Config, singleton, Logger
import time
import os
import pickle
import re

MONGODB = Config.MONGO_DICT
MONGODB_REMOTE = Config.MONGO_DICT_REMOTE
log = Logger().logger

@singleton
class MongoDatabase:

    def client(self):
        mongo = MongoClient(
            host=MONGODB['host'] if MONGODB['host'] else 'localhost',
            port=MONGODB['port'] if MONGODB['port'] else 27017,
            username=MONGODB['username'] if MONGODB['username'] else '',
            password=MONGODB['password'],
        )
        return mongo

    def db(self):
        return self.client()[MONGODB['db']]

    @staticmethod
    def upsert(collec: collection, condition: dict, data: dict):
        result = collec.find_one(condition)
        if result:
            collec.update_one(condition, {'$set': data})
            print('MONGO数据库《%s》中upsert更新: %s' % (collec.name, condition))
            return None
        else:
            collec.insert_one(data)
            print('MONGO数据库《%s》中upsert新增: %s' % (collec.name, condition))
            return condition

    @staticmethod
    def do_insert_one(collec: collection, condition: dict, data: dict):
        result = collec.find_one(condition)
        if result:
            print('MONGO数据库《%s》中do_insert_one已存在: %s' % (collec.name, condition))
            return None
        else:
            collec.insert_one(data)
            print('MONGO数据库《%s》中do_insert_one新增: %s' % (collec.name, condition))
            return condition


# 将数据从MONGO发送至远程云服务器上的MONGO中
def send_data_remote():
    time_start = time.perf_counter()
    print('===================================运行MongoDB发送数据: %s=========================================' % time_start)
    mongo = MongoDatabase().client()
    mongo_db = mongo[MONGODB['db']]
    collection_wealth = mongo_db['WEALTH']
    collection_text = mongo_db['TEXT']
    collection_manual = mongo_db['MANUAL']

    mongo_remote = MongoClient(host=MONGODB_REMOTE['host'], port=MONGODB_REMOTE['port'], username=MONGODB_REMOTE['username'], password=MONGODB_REMOTE['password'])
    mongo_remote_db = mongo_remote[MONGODB_REMOTE['db']]
    collection_remote_wealth = mongo_remote_db['WEALTH']
    collection_remote_text = mongo_remote_db['TEXT']
    collection_remote_manual = mongo_remote_db['MANUAL']
    try:
        do_send_data_remote(collection_wealth, collection_remote_wealth)
        do_send_data_remote(collection_text, collection_remote_text)
        do_send_data_remote(collection_manual, collection_remote_manual)
    except:
        print('===================================未能连接远程SERVER=======================================')
    time_end = time.perf_counter()
    print('===================================完成MongoDB数据发送: %s, 用时: %s=========================================' % (time_end, (time_end - time_start)))


def do_send_data_remote(collection_origin: collection, collection_remote: collection):
    origins = collection_origin.find({'status': {'$ne': 'done'}})
    if origins.count() > 0:
        for data in origins:
            condition = {'_id': data['_id']}
            collection_remote.update_one(condition, {'$set': data}, upsert=True)
            # 更新状态STATUS，从UNDO到0，1，2，到3时，设置为DONE，即发送3遍
            data = update_data_status(data)
            collection_origin.update_one(condition, {'$set': data})
            print('=============== 数据库中%s更新%s状态为%s。' % (collection_origin.name, data['_id'], data['status']))
    else:
        print('数据库中%s没有未上传的记录。' % collection_origin.name)


def update_data_status(data: dict):
    pattern_num = re.compile(r'\d')
    status = data['status']
    res = pattern_num.fullmatch(status)
    if not res:
        data['status'] = '0'
    else:
        num = int(res.group()) + 1
        data['status'] = str(num)
    if data['status'] == '3':
        data['status'] = 'done'
    return data


# 将数据导出为PICKLE格式
def dump_data_pickle(database: str, collection_name: str, filepath: str = Config.DATA_DIR):
    time_start = time.perf_counter()
    log.info('===================================开始执行dump_data_json: %s, %s=======================================' % (database, collection_name))
    mongo = MongoDatabase().client()
    db_origin = mongo[database]
    collection_origin = db_origin[collection_name]

    # datas = collection_origin.find()
    datas = collection_origin.find({'status': 'undo'})
    list_data = [data for data in datas]
    if list_data:
        filepath_prefix = filepath + '/' + time.strftime('%Y-%m-%d') + '/'
        os.makedirs(filepath_prefix, exist_ok=True)
        filename = filepath_prefix + collection_name + '.pkl'

        with open(filename, 'wb') as f:
            pickle.dump(list_data, f)

        for data in list_data:
            data['status'] = 'done'
            collection_origin.update_one({'_id': data['_id']}, {'$set': data})
    else:
        log.warning('database: %s, collection_name: %s 当中没有新增的数据。' % (database, collection_name))

    time_used = time.perf_counter() - time_start
    log.info('===================================结束dump_data_json, 用时：%s=======================================' % time_used)

