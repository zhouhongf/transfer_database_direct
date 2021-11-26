from database.base import Transfer
import re


class WealthTransfer(Transfer):
    table_name = 'wealth'

    def process_origin(self):
        datalist = []
        origins = self.collection.find({'status': {'$ne': 'done'}})
        # origins = self.collection.find()
        if origins.count() > 0:
            for one in origins:
                if 'file_type' in one.keys():
                    term = str(one['term'])
                    res_term = re.compile(r'\d+').fullmatch(term)
                    if not res_term:
                        one['term'] = 0
                    risk = str(one['risk'])
                    res_risk = re.compile(r'\d').fullmatch(risk)
                    if not res_risk:
                        one['risk'] = 0
                    datalist.append(one)
        return datalist


def start():
    print('------------------------------ Wealth 数据发送开始 ---------------------------------')
    # WealthTransfer().start()
