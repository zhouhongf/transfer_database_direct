from datetime import datetime
from config import Logger
from database.backends import MongoDatabase, MySQLDatabase
import re


class Transfer:
    logger = Logger(level='error').logger
    # MongoDB数据库和MySQL数据库的表名称要一致
    table_name = 'manual'

    def __init__(self):
        self.mysql_db = MySQLDatabase()
        self.mongo_db = MongoDatabase().db()
        self.collection = self.mongo_db[self.table_name.upper()]
        # self.mysql_db = SanicDatabase(self.config['host'], self.config['db'], self.config['user'], self.config['password'])

    def process_origin(self):
        datalist = []
        return datalist

    def start(self):
        start_time = datetime.now()
        print('----------- 开始时间：%s ------------' % start_time)
        datalist = self.process_origin()
        for one in datalist:
            self.handle_transfer_mysql(one)
        end_time = datetime.now()
        print('----------- 用时：%s ------------' % (end_time - start_time))

    def handle_transfer_mysql(self, data: dict):
        data_id = data['_id']
        data.pop('_id')
        data['id'] = data_id
        print('======准备处理%s：%s' % (self.table_name, data_id))
        d = self.mysql_db.table_has(table_name=self.table_name, field='id', value=data_id)
        if d:
            self.mysql_db.table_update(table_name=self.table_name, updates=data, field_where='id', value_where=data_id)
        else:
            self.mysql_db.table_insert(table_name=self.table_name, item=data)
        print('已处理完：%s' % data_id)
        self.process_update_record(data)

    def update_data_status(self, data: dict):
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

    def process_update_record(self, data: dict):
        data_update = self.update_data_status(data)
        data_id = data_update['id']
        data_update.pop('id')
        data_update['_id'] = data_id
        condition = {'_id': data_id}
        self.collection.update_one(condition, {'$set': data_update}, upsert=True)
        print('已更新完MONGO状态：%s, %s' % (self.table_name, data_id))
