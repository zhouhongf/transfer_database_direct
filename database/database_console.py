from importlib import import_module
from config import Config
import os
import time


def file_name(file_dir=os.path.join(Config.BASE_DIR, 'database/transfers')):
    all_files = []
    for file in os.listdir(file_dir):
        if file.endswith('_transfer.py'):
            all_files.append(file.replace('.py', ''))
    return all_files


# 需要导出三张表：MANUAL, WEALTH, TEXT
def database_console():
    # mongodb_module = import_module("database.backends.mongo_database")
    # mongodb_module.dump_data_pickle(database=Config.GROUP_NAME, collection_name='TEXT')
    # mongodb_module.dump_data_pickle(database=Config.GROUP_NAME, collection_name='WEALTH')
    # mongodb_module.dump_data_pickle(database=Config.GROUP_NAME, collection_name='MANUAL')
    # print('------------------------------ 数据导出完成 ---------------------------------')
    # mongodb_module.send_data_remote()
    print('------------------------------ 数据发送开始 ---------------------------------')
    all_files = file_name()
    for task in all_files:
        print('======================== task: %s' % task)
        task_module = import_module("database.transfers.{}".format(task))
        task_module.start()
        time.sleep(5)
    print('------------------------------ 数据发送完成 ---------------------------------')


if __name__ == '__main__':
    database_console()
