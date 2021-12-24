import pandas as pd
import datetime
import time
from pythonMySQL import *


def import_mysql_excel():
    success = M("success_result")

    null_dict = dict()
    null_dict["pay_user_rid"] = "昨"
    null_dict["status"] = "天"
    null_dict["orderNo"] = "暂"
    null_dict["orderNo"] = "无"
    null_dict["createTime"] = "数"
    null_dict["transferTime"] = "据"
    null_dict["tradeType"] = "3"
    null_dict["payeeAmount"] = '10'
    success_result = []
    s_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    success.field('pay_user_rid,status,orderNo,transferNumber,createTime,transferTime', 'tradeType',
                  'payeeAmount').where('transferTime>').select()

    if not success_result:
        success_result.append(null_dict)

    pf = pd.DataFrame(success_result)
    order = ["pay_user_rid", "status", "orderNo", "transferNumber", "createTime", "transferTime", "tradeType",
             "payeeAmount"]
    pf = pf[order]
    file_path = pd.ExcelWriter(
        f'/success_excel/success-{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}.xlsx')

    pf.fillna(' ', inplace=True)

    pf.to_excel(file_path, encoding='utf-8', index=False, sheet_name="sheet1")
    file_path.save()

    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
