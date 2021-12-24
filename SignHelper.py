# -*- coding: utf-8 -*-
import hashlib
import json


class ApiSign(object):

    def data_processing(self, data):
        """
        data: 需要签名的数据，字典类型
        return: 处理后的字符串， 格式为：参数名称=参数值，并用&连接
        """
        res2_json = json.dumps(data)
        res_dict = json.loads(res2_json)

        dataList = []
        for k in sorted(res_dict):
            dataList.append(("%s=%s" % (k, data[k])))
        return "&".join(dataList)

    def md5_sign(self, res_dict):
        """
        MD5签名
        app_pwd: MD5签名需要的字符串
        return: 签名后的字符串api_sign
        """
        data = self.data_processing(res_dict)
        print()
        md5 = hashlib.md5()
        md5.update(data.encode("utf-8"))
        return md5.hexdigest()


# data = {
#     "orderNo": "1640324755620941",
# }
# sign = ApiSign()
# s = sign.md5_sign(data)
# print(s)
