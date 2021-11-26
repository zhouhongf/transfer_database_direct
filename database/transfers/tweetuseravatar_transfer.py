from database.base import Transfer


class TweetuseravatarTransfer(Transfer):
    table_name = 'user_avatar'

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
    print('------------------------------ TweetUserAvatar 数据发送开始 ---------------------------------')
    TweetuseravatarTransfer().start()
