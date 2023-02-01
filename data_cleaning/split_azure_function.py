import os
import csv
import json
import numpy as np
import matplotlib.pyplot as plt


def read_file_by_dir(dir_name, keyword):
    count = 0
    file_name_list = []
    file_list = os.listdir(dir_name)
    # print ("file=", file_list)
    for file_name in sorted(file_list):
        if file_name.find(".gitignore") == -1 and file_name.find(keyword) != -1:
            file_name_list.append(file_name)
    file_num = len(file_name_list)
    print ("- Get %s files in Dir: %s" % (file_num, dir_name))
    return file_name_list


def read_file(dir_name, file_name):
    data_output = {}
    afile_name = file_name.split(".")[-2]
    data_output[afile_name] = {}
    sub_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                # print (row)
                owner = row["HashOwner"]
                data_output[afile_name][owner] = {}

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                owner = row["HashOwner"]
                app = row["HashApp"]

                data_output[afile_name][owner][app] = {}

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                owner = row["HashOwner"]
                app = row["HashApp"]
                function = row["HashFunction"]
                data_output[afile_name][owner][app][function] = {}

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                owner = row["HashOwner"]
                app = row["HashApp"]
                function = row["HashFunction"]
                trigger = row["Trigger"]
                data_output[afile_name][owner][app][function][trigger] = []

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                owner = row["HashOwner"]
                app = row["HashApp"]
                function = row["HashFunction"]
                trigger = row["Trigger"]
                for i in range(1440):
                    value = int(row["%s" % (i+1)])
                    data_output[afile_name][owner][app][function][trigger].append(value)
    except Exception as e:
        print ("failed to read files: %s" % str(e))
    return data_output


def write_data_info(file_name, owner, app, function, trigger, output):
    file_name1 = "./azure-functions-dataset2019-d14/%s-%s-%s-%s-%s_output.json" % (file_name, owner, app, function, trigger)
    data = {}
    data["title"] = "%s_%s_%s_%s" % (owner, app, function, trigger)
    data['description'] = "owner=%s,app=%s, function=%s, trigger=%s" % (owner, app, function, trigger)
    data['data'] = output
    try:
        data_output = json.dumps(data, indent=4)
        with open(file_name1, "w", encoding='utf-8') as f:
            f.write(data_output)
    except Exception as e:
        print ("current=", os.getcwd())
        print ("failed to write azure functions: %s" % str(e))
        return False
    return True


def main():
    dir_name = "./azurefunctions-dataset2019"
    file_name_list = read_file_by_dir(dir_name, "invocations")
    print (file_name_list)
    count = 0
    correct_count = 0
    error_count = 0
    for file_name in file_name_list:
        print (file_name)
        if file_name.find("d14") != -1:
            data_output = read_file(dir_name, file_name)
            for afile_name in data_output.keys():
                for owner in data_output[afile_name].keys():
                    for app in data_output[afile_name][owner].keys():
                        for function in data_output[afile_name][owner][app].keys():
                            for trigger in data_output[afile_name][owner][app][function].keys():
                                output = data_output[afile_name][owner][app][function][trigger]
                                print (count)
                                res = write_data_info(afile_name, owner, app, function, trigger, output)
                                count += 1
                                if res:
                                    correct_count += 1
                                else:
                                    error_count += 1
        # break
    print (count, correct_count, error_count)

if __name__ == '__main__':
    main()
