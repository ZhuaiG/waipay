import pandas as pd
import time
from pythonMySQL import *

SuccessResult = M("success_result")
FailResult = M("fail_result")


def import_mysql_success_excel():
    null_dict = dict()
    null_dict["pay_user_rid"] = "昨"
    null_dict["status"] = "天"
    null_dict["orderNo"] = "暂"
    null_dict["transferNumber"] = "无"
    null_dict["createTime"] = "数"
    null_dict["transferTime"] = "据"
    null_dict["tradeType"] = "3"
    null_dict["payeeAmount"] = '10'

    success_result = SuccessResult.field(
        'pay_user_rid,status,orderNo,transferNumber,createTime,transferTime,tradeType,payeeAmount').where(
        "id>'1'").select()

    if not success_result:
        success_result.append(null_dict)

    pf = pd.DataFrame(success_result)
    order = ["pay_user_rid", "status", "orderNo", "transferNumber", "createTime", "transferTime", "tradeType",
             "payeeAmount"]
    pf = pf[order]
    file_path = pd.ExcelWriter(
        f'success_excel/success-{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}.xlsx')

    pf.fillna(' ', inplace=True)

    pf.to_excel(file_path, encoding='utf-8', index=False, sheet_name="sheet1")
    file_path.save()

    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


def import_mysql_fail_excel():
    null_dict = dict()
    null_dict["id"] = "昨"
    null_dict["pay_user_rid"] = "天"
    null_dict["status"] = "暂"
    null_dict["errorMessage"] = "无"
    null_dict["orderNo"] = "1"

    fail_result = FailResult.field('id,pay_user_rid,status,errorMessage,orderNo').where("id>'1'").select()

    if not fail_result:
        fail_result.append(null_dict)

    pf = pd.DataFrame(fail_result)
    order = ["id", "pay_user_rid", "status", "errorMessage", "orderNo"]
    pf = pf[order]
    file_path = pd.ExcelWriter(
        f'fail_excel/fail-{time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))}.xlsx')

    pf.fillna(' ', inplace=True)

    pf.to_excel(file_path, encoding='utf-8', index=False, sheet_name="sheet1")
    file_path.save()

    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))


def import_result():
    import_mysql_fail_excel()
    import_mysql_success_excel()


if __name__ == "__main__":
    import_result()
