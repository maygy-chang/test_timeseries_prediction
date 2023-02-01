import os
import sys
import time
import json
import math
import shutil
import numpy as np
import statistics
from define import metrics_path


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


def main(dir_name, cluster_name):
    file_name_list = read_file_by_dir(dir_name)
    count = 0
    large_3stdev_count = 0
    two_to_3stedv_count = 0
    one_to_2stedv_count = 0
    less_1stdev_count = 0
    others_count = 0
    error_count = 0
    is_nan = False
    new_dir_name = "total_metrics_%s" % cluster_name
    if not os.path.isdir(new_dir_name):
        os.makedirs(new_dir_name)
    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        data_output = read_file(dir_name, file_name)
        data = data_output["data"]
        try:
            maximum = max(data)
            mean = sum(data)/len(data)
            stdev = np.std(np.array(data), ddof=1)
            diff = maximum - mean
            print count, file_name, "mean=", mean, "max=", maximum, "stdev=", stdev, "diff=", diff
            if len(data) < 150:
                raise Exception("data is insufficient")
            if diff >= 3*stdev:
                is_nan = False
                print "- >=3*Stdev"
                large_3stdev_count += 1
            elif diff >= 2*stdev and diff < 3*stdev:
                is_nan = False
                print "- 2~3*Stdev"
                two_to_3stedv_count += 1
            elif diff >= 1*stdev and diff < 2*stdev:
                is_nan = False
                print "- 1~2*Stdev"
                one_to_2stedv_count += 1
            elif diff < 1*stdev:
                is_nan = False
                print "- <1*Stdev"
                less_1stdev_count += 1
            else:
                is_nan = True
                print "- Others"
                others_count += 1
            count += 1
            if not is_nan:
                print "- Total"
                shutil.copyfile("%s/%s" % (dir_name, file_name), "./%s/%s-%s" % (new_dir_name, cluster_name, file_name))
            print ("\n")
        except Exception as e:
            print "- Error=", file_name, str(e)
            error_count += 1
            pass
        print "Total=", count, ">3stdev=", large_3stdev_count, "2-3stdev=", two_to_3stedv_count, "1-2stdev=", one_to_2stedv_count, "<1stdev=", less_1stdev_count, "others=", others_count, "errors=", error_count

  


if __name__ == "__main__":
    try:
        dir_name = sys.argv[1]
        cluster_name = sys.argv[2]
        main(dir_name, cluster_name)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python collect_data_by_file.py")
        print ("- dir_name: <source dir>")
        print ("- data_type: confirmed, deaths, recovered")
