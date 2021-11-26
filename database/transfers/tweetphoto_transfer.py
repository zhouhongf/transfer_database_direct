from database.base import Transfer


class TweetphotoTransfer(Transfer):
    table_name = 'tweet_photo'

    def process_origin(self):
        datalist = []
        origins = self.collection.find({'status': {'$ne': 'done'}})
        # origins = self.collection.find()
        if origins.count() > 0:
            for one in origins:
                print('id: %s' % one['_id'])
                datalist.append(one)
        return datalist


def start():
    print('------------------------------ TweetPhoto 数据发送开始 ---------------------------------')
    TweetphotoTransfer().start()
