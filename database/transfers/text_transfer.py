from database.base import Transfer


class TextTransfer(Transfer):
    table_name = 'text'

    def process_origin(self):
        datalist = []
        origins = self.collection.find({'status': {'$ne': 'done'}})
        # origins = self.collection.find()
        if origins.count() > 0:
            for one in origins:
                print('name: %s' % one['name'])
                datalist.append(one)
        return datalist


def start():
    print('------------------------------ Text 数据发送开始 ---------------------------------')
    # TextTransfer().start()
