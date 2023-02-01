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
    afile_name = file_name
    data_output[afile_name] = {}
    sub_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                msname = row["msname"]
                data_output[afile_name][msname] = {}
                data_output[afile_name][msname]["cpu"] = {}
                data_output[afile_name][msname]["memory"] = {}

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                timestamp = int(row["timestamp"])
                msname = row["msname"]
                data_output[afile_name][msname]["cpu"][timestamp] = 0
                data_output[afile_name][msname]["memory"][timestamp] = 0

        with open(sub_file_name) as file:
            rows = csv.DictReader(file)
            for row in rows:
                timestamp = int(row["timestamp"])
                msname = row["msname"]
                cpu = 0
                memory = 0
                if row.get("instance_cpu_usage"):
                    cpu = float(row["instance_cpu_usage"])
                if row.get("instance_memory_usage"):
                    memory = float(row["instance_memory_usage"])
                data_output[afile_name][msname]["cpu"][timestamp] += cpu
                data_output[afile_name][msname]["memory"][timestamp] += memory
    except Exception as e:
        print ("failed to read files: %s" % str(e))
    return data_output


def write_data_info(file_name, msname, resource, output):
    file_name1 = "./alibaba-app-cpu0-11-2021/%s-%s-%s_output.json" % (file_name, msname, resource)
    if resource == "memory":
        file_name1 = "./alibaba-app-memory0-11-2021/%s-%s-%s_output.json" % (file_name, msname, resource)
    data = {}
    data["title"] = "%s_%s" % (msname, resource)
    data['description'] = "app=%s, resource=%s" % (msname, resource)
    data['data'] = output
    try:
        data_output = json.dumps(data, indent=4)
        with open(file_name1, "w", encoding='utf-8') as f:
            f.write(data_output)
    except Exception as e:
        print ("current=", os.getcwd())
        print ("failed to write alibaba workloads: %s" % str(e))
        return False
    return True


def main():
    dir_name = "./clusterdata/cluster-trace-microservices-v2021/data/MSResource"
    file_name_list = read_file_by_dir(dir_name, ".csv")
    print (file_name_list)
    count = 0
    correct_count = 0
    error_count = 0
    data_output = {}
    for i in range(12):
        for file_name in file_name_list:
            if file_name.find("MSResource_%s.csv" % i) != -1:
                print (file_name)
                data_output[i] = read_file(dir_name, file_name)

    for afile_name in data_output[0].keys():
        for msname in data_output[0][afile_name].keys():
            for resource in data_output[0][afile_name][msname].keys():
                output = {}
                output1 = []
                for i in range(12):
                    file_name = "MSResource_%s.csv" % i
                    if msname in data_output[i][file_name].keys():
                        data_list = list(data_output[i][file_name][msname][resource].values())
                        print (i, "file_name = ", data_output[i].keys(), "data_length=", len(data_list))
                        output1 += data_list
                    else:
                        print ("- Cannot find App(%s)" % msname)
                        data_list = list(np.arange(120))
                        output1 += data_list
                print (count, msname, "data_length", len(output1))
                res = write_data_info(afile_name, msname, resource, output1)
                count += 1
                if res:
                    correct_count += 1
                else:
                    error_count += 1
    print ("Total=", count, correct_count, error_count)


if __name__ == '__main__':
    main()
