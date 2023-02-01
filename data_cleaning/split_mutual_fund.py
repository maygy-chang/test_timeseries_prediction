import os
import csv
import json
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime


def read_file_by_dir(dir_name):
    count = 0
    file_name_list = []
    file_list = os.listdir(dir_name)
    for file_name in sorted(file_list):
        if file_name.find("Mutual") != -1 and file_name.find("prices") != -1:
            file_name_list.append(file_name)
    print ("- Get files: %s" % (file_name_list))
    return file_name_list


def get_fund_by_file(dir_name, file_name, data_output):
    sub_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                fund_name = row.get("fund_symbol")
                if fund_name not in data_output.keys():
                    data_output[fund_name] = {}
                price_date = row.get("price_date")
                nav_per_share = row.get("nav_per_share")
                timestamp = get_timestamp(price_date)
                data_output[fund_name][timestamp] = nav_per_share
    except Exception as e:
        print ("Failed to read %s: %s" % (sub_file_name, str(e)))
    fund_num = len(data_output.keys())
    print ("Total Fund(%s) in File(%s)" % (fund_num, file_name))
    total_time = get_total_time(data_output)
    print ("Total Time(%s) in Data(%s)" % (total_time, file_name))
    return data_output


def get_total_time(data_output):
    total_time = 0
    for fund_name in data_output.keys():
        total_time += len(data_output[fund_name].keys())
    return total_time


def get_timestamp(data_time):
    year = int(data_time.split("-")[0])
    month = int(data_time.split("-")[1])
    date = int(data_time.split("-")[2])
    hour = 0
    min = 0
    timestamp = int(datetime(year, month, date, hour, min).timestamp())
    return timestamp


def write_data_info(fund_name, output):
    file_name = "./mutual-fund-1973-2021/%s-1973-2021-jun-nov_output.json" % (fund_name)
    data = {}
    data["title"] = "%s-1973/05/03-2021/11/30" % (fund_name)
    data['description'] = "%s-1973/05/03-2021/11/30" % (fund_name)
    data['data'] = output
    try:
        data_output = json.dumps(data, indent=4)
        with open(file_name, "w", encoding='utf-8') as f:
            f.write(data_output)
    except Exception as e:
        print ("current=", os.getcwd())
        print ("Failed to write file(%s): %s" % (file_name, str(e)))
        return False
    print ("Success to write file(%s)" % file_name)
    return True


def fill_data(data_list):
    start_time = get_timestamp("1973-05-03")
    end_time = get_timestamp("2021-11-30")
    for timestamp in range(start_time, end_time):
        if timestamp % 86400 == 0 and timestamp not in data_list:
            data_list[timestamp] = 0
    return data_list


def main():
    dir_name = "mutual-fund"
    data_output = {}
    file_name_list = read_file_by_dir(dir_name)
    for file_name in file_name_list:
        data_output = get_fund_by_file(dir_name, file_name, data_output)

    for fund_name in data_output.keys():
        data_list = data_output[fund_name]
        data_list = fill_data(data_list)
        write_data_info(fund_name, data_list)


if __name__ == '__main__':
    main()
