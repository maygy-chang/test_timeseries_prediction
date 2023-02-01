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
    # print ("file=", file_list)
    for i in range(6, 12):
        for file_name in sorted(file_list):
            if file_name.find("data_%s" % i) != -1 and file_name.find(".csv") != -1 and file_name not in file_name_list:
                file_name_list.append(file_name)
    print ("- Get files: %s" % (file_name_list))
    return file_name_list


def get_bs_loc_by_row(row):
    base_loc = row.get("location(latitude/lontitude)")
    base_loc1 = row.get("latitude")
    base_loc2 = row.get("longitude")
    if base_loc1 and base_loc2:
        base_loc = "%s/%s" % (base_loc1, base_loc2)
    return base_loc


def get_bs_list_by_file(dir_name, file_name, base_loc_list):
    sub_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                base_loc = get_bs_loc_by_row(row)
                if base_loc not in base_loc_list and base_loc:
                    base_loc_list.append(base_loc)
    except Exception as e:
        print ("Failed to read %s: %s" % (sub_file_name, str(e)))
    bs_num = len(base_loc_list)
    print ("Total BS(%s) in Date(%s)" % (bs_num, file_name))
    return base_loc_list


def get_total_time(data_output):
    total_time = 0
    for base_id in data_output.keys():
        total_time += len(data_output[base_id].keys())
    return total_time


def get_timestamp(data_time):
    year = 0
    month = 0
    date = 0
    hour = 0
    min = 0
    if len(data_time.split()) == 1:
        year = int(data_time.split()[0].split("/")[0])
        month = int(data_time.split()[0].split("/")[1])
        date = int(data_time.split()[0].split("/")[2])
        hour = 0
        min = 0
    if len(data_time.split()) == 2:
        year = int(data_time.split()[0].split("/")[0])
        month = int(data_time.split()[0].split("/")[1])
        date = int(data_time.split()[0].split("/")[2])
        hour = int(data_time.split()[1].split(":")[0])
        min = int(data_time.split()[1].split(":")[1])
    timestamp = int(datetime(year, month, date, hour, min).timestamp())
    return timestamp


def get_start_time_list_by_file(dir_name, file_name, base_loc_list, data_output):
    sub_file_name = "%s/%s" % (dir_name, file_name)
    base_id = -1
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                base_loc = get_bs_loc_by_row(row)
                start_time = row.get("start time")
                start_timestamp = get_timestamp(start_time)
                if base_loc in base_loc_list:
                    base_id = base_loc_list.index(base_loc)
                    data_output[base_id][start_timestamp] = 0
    except Exception as e:
        print ("Failed to read %s: %s" % (sub_file_name, str(e)))

    total_time = get_total_time(data_output)
    print ("Total Time(%s) in Data(%s)" % (total_time, file_name))
    return data_output


def get_user_list_by_file(dir_name, file_name, base_loc_list, data_output):
    sub_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                base_loc = get_bs_loc_by_row(row)
                user_id = row.get("user id")
                start_time = row.get("start time")
                start_timestamp = get_timestamp(start_time)
                if base_loc in base_loc_list:
                    base_id = base_loc_list.index(base_loc)
                    data_output[base_id][start_timestamp] += 1
    except Exception as e:
        print ("Failed to read %s: %s" % (sub_file_name, str(e)))
    return data_output


def write_data_info(bs_id, output):
    file_name = "./telecom-dataset2014-jun-nov/bs%s-2014-jun-nov_output.json" % (bs_id)
    data = {}
    data["title"] = "bs%s-2014-0601-1130" % (bs_id)
    data['description'] = "bs%s-2014-0601-1130" % (bs_id)
    data['data'] = output
    try:
        data_output = json.dumps(data, indent=4)
        with open(file_name, "w", encoding='utf-8') as f:
            f.write(data_output)
    except Exception as e:
        print ("current=", os.getcwd())
        print ("Failed to write file(%s): %s" % (file_name, str(e)))
        return False
    data_num = len(output.keys())
    print ("Success to write %s data to file(%s)" % (data_num, file_name))
    return True


def fill_data(data_list):
    start_time = get_timestamp("2014/06/01")
    end_time = get_timestamp("2014/12/01")
    for timestamp in range(start_time, end_time):
        if timestamp % 60 == 0 and timestamp not in data_list:
            data_list[timestamp] = 0
    return data_list


def main():
    dir_name = "dataset-telecom"
    file_name_list = read_file_by_dir(dir_name)
    base_loc_list = []
    data_output = {}
    # Get BS
    for file_name in file_name_list:
        print ("Get BS by file(%s)" % file_name)
        base_loc_list = get_bs_list_by_file(dir_name, file_name, base_loc_list)
    print ("Total BS=%s" % len(base_loc_list))

    # Get Time
    for base_loc in base_loc_list:
        base_id = base_loc_list.index(base_loc)
        data_output[base_id] = {}

    for file_name in file_name_list:
        print ("Get Start Time by file(%s)" % file_name)
        data_output = get_start_time_list_by_file(dir_name, file_name, base_loc_list, data_output)
    total_time = get_total_time(data_output)
    print ("Total Time=%s" % (total_time))

    # Get User
    for file_name in file_name_list:
        print ("Get Users by file(%s)" % file_name)
        data_output = get_user_list_by_file(dir_name, file_name, base_loc_list, data_output)

    # Fill Data & Write Data
    for base_id in data_output.keys():
        data_list = fill_data(data_output[base_id])
        write_data_info(base_id, data_list)


if __name__ == '__main__':
    main()
