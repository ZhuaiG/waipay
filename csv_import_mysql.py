from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from pythonMySQL import *
import pandas as pd
import os
import datetime
from time import sleep
import logging
import requests
from SignHelper import ApiSign
from redis import Redis


# 网站获取csv文件
def get_csv():
    chrome_options = Options()
    chrome_options.add_argument("--headless")

    driver = webdriver.Chrome(chrome_options=chrome_options)

    driver.get('https://pages.landingcube.com/login/')

    login_inputs = driver.find_elements_by_class_name('input')

    login_inputs[0].send_keys("irebateio@gmail.com")

    login_inputs[1].send_keys("uiP5LQJ3sZT!cpJ")

    driver.find_element_by_id('login-submit').click()
    print("***********登录成功**************")

    sleep(2)

    driver.find_element_by_xpath('//*[@id="dashboard-header"]/a[3]').click()

    sleep(2)

    driver.find_element_by_xpath('//*[@id="download-csv-button"]').click()
    print("*************下载成功*************")

    while True:  # 等待文件下载完成
        sleep(2)
        if os.path.exists('customers.csv'):
            break
    update_new_data()


PayUser = M("payuser")


# 将csv中最新的数据插入数据库
def update_new_data():
    df = pd.read_csv('customers.csv')

    count = 0

    max_time_user = PayUser.where("rid > 1").order("time desc").limit(1).select()
    if len(max_time_user) != 0:
        max_time = str(max_time_user[0]["time"])
    else:
        max_time = '2021-12-19 19:58:58'
        print("get mysql data is null")

    while True:
        if df["time"][count] == max_time:
            break
        count += 1

    df2 = pd.read_csv('customers.csv', nrows=count, keep_default_na=False)

    all_user_pay = []

    for i in range(count):
        user_pay_dict = dict()
        user_pay_dict["email"] = df2["email"][i]
        user_pay_dict["fname"] = df2["fname"][i]
        user_pay_dict["lname"] = df2["lname"][i]
        user_pay_dict["time"] = df2["time"][i]
        user_pay_dict["ip"] = df2["ip"][i]
        user_pay_dict["campaign"] = df2["campaign"][i]
        user_pay_dict["code"] = df2["code"][i]
        user_pay_dict["asin"] = df2["asin"][i]
        user_pay_dict["source"] = df2["source"][i]
        user_pay_dict["integration_status"] = df2["integration_status"][i].replace(" ", "-")
        user_pay_dict["status"] = df2["status"][i].replace(" ", "-")
        user_pay_dict["consent_status"] = df2["consent_status"][i]
        user_pay_dict["consent_msg"] = df2["consent_msg"][i]
        user_pay_dict["rebate_amount"] = float(df2["rebate_amount"][i])
        user_pay_dict["gclid"] = df2["gclid"][i]

        all_user_pay.append(user_pay_dict)
    if len(all_user_pay) != 0:
        PayUser.addAll(all_user_pay[::-1])
        os.remove('customers.csv')  # 更新数据后删除csv
        logging.info(datetime.datetime.now(), "新数据更新成功")

        # 开始转账
        post_order()
    else:
        logging.info(datetime.datetime.now(), "暂无新数据更新")


SuccessResult = M("success_result")
FailResult = M("fail_result")


# 调用转账结果接口，存入转账信息
def check_order_status():
    # 获取access_token
    conn = Redis(host='localhost', port=6379, db=0)
    access_token = conn.get('access_token')

    if access_token is None:
        access_token = grant_authorization()
        conn.set(name='access_token', value=access_token, ex=600)

    check_order_url = 'http://dev.payout.vip/mbs/api/outerFinance/checkTransferResult'
    pay_user_list = PayUser.where('consent_status="TRANSFERING" and order_no!=""').select()
    headers = {"access_token": access_token}
    for pay_user in pay_user_list:
        data = {'orderNo': pay_user["order_no"]}

        get_sign = ApiSign()
        sign = get_sign.md5_sign(data)
        data['sign'] = sign

        req = requests.post(check_order_url, json=data, headers=headers)
        get_data = req.json()
        print("********check_order_status******* 请求返回的数据", get_data)
        is_success = get_data["success"]
        if is_success is True:
            status = get_data["content"]["status"]
            if status == "SUCCESS":
                pay_user_updata = dict()
                success_insert = dict()
                # 修改payuser表转账状态
                pay_user_updata["rid"] = pay_user["rid"]
                pay_user_updata["consent_status"] = status
                PayUser.save(pay_user_updata)
                # 将转账成功的接口数据存入success_result表
                success_insert["pay_user_rid"] = pay_user["rid"]
                success_insert.update(get_data["content"])
                PayUser.addAll(success_insert)

            elif status == "FAIL":
                # 转账失败将失败信息存入数据库
                fail_insert = dict()
                fail_insert["pay_user_rid"] = pay_user["rid"]
                fail_insert.update(get_data["content"])
            else:
                logging.info("rid为", pay_user["rid"], "查看转账结果接口其状态为转账中")
        else:
            logging.info("查看转账结果接口调用失败rid为", pay_user["rid"], "接口失败原因:", get_data["errmsg"])


# 调用登录接口
def login_to_get(app_id, key):
    login_url = 'http://dev.payout.vip/mbs/pub/user/getOuterAccessToken'
    data = {'appId': app_id, 'key': key}
    req = requests.post(login_url, json=data)
    get_data = req.json()
    access_token = get_data["content"]
    print("*********获取token成功***************")
    return access_token


# 调用获取权限接口
def grant_authorization():
    grant_authorization_url = 'http://dev.payout.vip/mbs/pub/user/authOuterKey'
    data = {'username': '18908406059', 'password': 'abcd1234'}
    req = requests.post(grant_authorization_url, json=data)
    get_data = req.json()
    app_id = get_data["content"]["appId"]
    key = get_data["content"]["key"]
    return login_to_get(app_id, key)


# 调用转账接口
def post_order():
    # 获取access_token
    conn = Redis(host='localhost', port=6379, db=0)
    access_token = conn.get('access_token')

    if access_token is None:
        access_token = grant_authorization()
        conn.set(name='access_token', value=access_token, ex=600)
    commit_order_url = 'http://dev.payout.vip/mbs/api/outerFinance/postTransferOrder'
    pay_user_list = PayUser.where('integration_status="received-by-aweber" and fname!="" and rebate_amount!=0').order(
        "time desc").select()
    for pay_user in pay_user_list:
        data = {'payeeName': pay_user["fname"], 'payeeAccount': pay_user["email"],
                'payeeAmount': pay_user["rebate_amount"],
                'payeeCurrency': 'USD', 'bizNumber': str(pay_user["rid"]),
                'tradeType': "3", 'remark': 'rst'}
        get_sign = ApiSign()
        sign = get_sign.md5_sign(data)

        data['sign'] = sign

        headers = {"access_token": access_token}
        req = requests.post(commit_order_url, json=data, headers=headers)
        sleep(2)
        get_data = req.json()
        print("********post_order******* 请求返回的数据", get_data)
        is_success = get_data["success"]
        if is_success is True:
            logging.info(datetime.datetime.now(), f"用户rid{pay_user['rid']}转账成功")
            order_no = get_data["content"]["orderNo"]
            update_dict = dict()
            update_dict["rid"] = pay_user["rid"]
            update_dict["consent_status"] = "TRANSFERING"
            update_dict["status"] = "email-opened"
            update_dict["order_no"] = order_no
            PayUser.save(update_dict)
        else:
            logging.info("转账接口调用失败rid为", pay_user["rid"], "接口失败原因:", get_data["errmsg"])


if __name__ == "__main__":
    check_order_status()
