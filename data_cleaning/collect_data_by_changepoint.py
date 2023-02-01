import os
import sys
import time
import json
import math
import shutil
import numpy as np
import statistics
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from define import metrics_path, obs_data_length
from collect_data_by_seasonality_changepoint import get_change_point


def read_file_by_dir(dir_name):
    count = 0
    file_name_list = []
    file_list = os.listdir(dir_name)
    for file_name in sorted(file_list):
        file_name_list.append(file_name)
    print ("- Get %s files in Dir: %s" % (len(file_name_list), dir_name))
    return file_name_list


def read_file(dir_name, file_name):
    print ("- Read File: %s/%s" % (dir_name, file_name))
    data_output = {}
    detailed_file_name = "./%s/%s" % (dir_name, file_name)
    try:
        with open(detailed_file_name) as file:
            data_output = json.load(file)
    except Exception as e:
        print ("failed to read file(%s): %s" % (detailed_file_name, str(e)))
    return data_output
    

def get_picture(data, dir_name, file_name, changepoint_list):
    start = 0
    x = np.arange(len(data))
    plt.figure(figsize=(8, 4))
    plt.plot(x, data, label="data", color="k", linewidth=1)
    changepoint_data = []
    ylim = max(data)*1.5
    for i in range(len(data)):
        if i in changepoint_list:
            changepoint_data.append(ylim)
        else:
            changepoint_data.append(0)
    plt.plot(x, changepoint_data, label="changepoint", color="r", linestyle=":", marker='o',linewidth=0.5)           

    title_name = "Part1=%s-Part2=%s-Part3=%s" % (file_name.split("-")[-3], file_name.split("-")[-2], file_name.split("-")[-1])
    plt.title("%s" % title_name)
    plt.ylim(0, ylim)
    plt.legend(loc=0)
    plt.savefig("%s/%s.png" % (dir_name, file_name))
    plt.close()


def main(dir_name, cluster_name):
    file_name_list = read_file_by_dir(dir_name)
    count = 0
    others_count = 0
    error_count = 0
    changepoint_count = 0
    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        data_output = read_file(dir_name, file_name)
        total_data = data_output["data"]
        data = total_data[:obs_data_length]
        try:
            is_changepoint, part1_count, part2_count, part3_count, changepoint_list = get_change_point(dir_name, file_name)
            if len(total_data) < 150:
                raise Exception("data is insufficient")

            if is_changepoint:
                print ("- Find Change Point")
                changepoint_count += 1
                new_file_name = "%s" % (file_name)
                new_file_name += "-%s-%s-%s" % (part1_count, part2_count, part3_count)
                get_picture(data, "changepoint_picture", new_file_name, changepoint_list)

            else:
                print ("- Others")
                others_count += 1
            count += 1
            print ("- Total")
            print ("\n")
        except Exception as e:
            print ("- Error=", file_name, str(e))
            error_count += 1
            break

        print ("Total=", count, "errors=", error_count, "change_point=", changepoint_count, "\n")

  


if __name__ == "__main__":
    try:
        dir_name = sys.argv[1]
        cluster_name = sys.argv[2]
        main(dir_name, cluster_name)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python collect_data_by_changepoint.py")
        print ("- dir_name: <source_data>")
        print ("- cluster_name: d01")

