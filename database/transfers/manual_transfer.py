from database.base import Transfer


class ManualTransfer(Transfer):
    table_name = 'manual'

    def process_origin(self):
        datalist = []
        origins = self.collection.find({'status': {'$ne': 'done'}})
        # origins = self.collection.find({'bank_name': '中信银行'})
        if origins.count() > 0:
            for one in origins:
                file_suffix = one['file_suffix']
                if file_suffix not in ['.pdf', '.doc', '.docx']:
                    str_value = one['content']
                    print('content的类型是：%s' % type(str_value))
                    if isinstance(str_value, str):
                        one['content'] = bytes(str_value, encoding='utf-8')
                datalist.append(one)
        return datalist


def start():
    print('------------------------------ Manual 数据发送开始 ---------------------------------')
    # ManualTransfer().start()
