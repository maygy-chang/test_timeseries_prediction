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
import logging
from statsmodels.tsa.stattools import adfuller


class AITech:

    def __init__(self):
        pass

    def do_adfuller_test(self, timeseries, method='AIC'):
        timeseries = np.squeeze(timeseries)
        num_nobs = timeseries.shape[0] - 1
        if num_nobs <= 4:
            return True
        try:
            threshold = 12 * (num_nobs / 100) ** (1 / 4)
            if num_nobs > threshold:
                dftest = adfuller(timeseries, maxlag=np.floor(threshold).astype(int), autolag=method)
            else:
                dftest = adfuller(timeseries, maxlag=num_nobs - 1, autolag=method)
            return bool(dftest[1] < 0.05)
        except Exception:
            # TODO: log warning
            return False

    def difference(self, dataset, interval):
        diff = list()
        for i in range(interval, len(dataset)):
            value = dataset[i] - dataset[i - interval]
            diff.append(value)
        return diff


def check_data_trend(data_list):
    d = 0
    a = AITech()
    try:
        for i in range(10):
            is_stationary = a.do_adfuller_test(data_list, method='AIC')
            if is_stationary:
                d = i
                break
            else:
                data_list = a.difference(data_list, i)
        print ("- Data Trend d = %s" % d)
    except Exception as e:
        print ("Failed to Check Trend: %s" % str(e))
        d = -1
        pass
    return d


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


def get_picture(data, dir_name, file_name):
    print ("- Get %s to Convert Picture" % file_name)
    start = 0
    x = np.arange(len(data))
    ylim = max(data)
    plt.figure(figsize=(8, 4))
    plt.plot(x, data, label="%s" % file_name, color="k", linewidth=1)
    plt.title("%s" % file_name)
    plt.ylim(0, ylim)
    plt.legend(loc=0)
    plt.savefig("%s/%s.png" % (dir_name, file_name))
    plt.close()


def generate_data_by_points(data_list, points):
    new_data_list = []
    for i in range(len(data_list)):
        if i % points == 0:
            data = data_list[i]
            new_data_list.append(data)
    print ("- Generate New Data(%s) by Data Points=%s" % (len(new_data_list), points))
    return new_data_list


def write_data_info(dir_name, file_name, points, data_list):
    new_dir_name = "%s-%sm" % (dir_name, points)
    if not os.path.isdir(new_dir_name):
        os.mkdir(new_dir_name)
    data = {}
    new_file_name = "%s/%s-%sm.json" % (new_dir_name, file_name, points)
    data["title"] = "%s-%sp" % (file_name, points)
    data['description'] = "%s-%sm" % (file_name, points)
    data['data'] = data_list
    try:
        data_output = json.dumps(data, indent=4)
        with open(new_file_name, "w", encoding='utf-8') as f:
            f.write(data_output)
    except Exception as e:
        print ("current=", os.getcwd())
        print ("- Failed to write %s: %s" % (new_file_name, str(e)))
    print ("- Success to write %s" % new_file_name)


def get_chunk_data_list(data_list, chunk_size):
    chunked_list = []
    for i in range(0, len(data_list), chunk_size):
        chunked_list.append(data_list[i:i+chunk_size])
    print ("- Get %s Chunk Data" % len(chunked_list))
    return chunked_list


def check_data_length(new_data_list, max_data_length):
    if len(new_data_list) < max_data_length:
        raise Exception("- The data length(%s) should >= %s" % (len(new_data_list), max_data_length))
    print ("- Check Data Length(%s)" % len(new_data_list))


def discrete_data(data_list, zero_threshold):
    is_zero = False
    new_zero_threshold = int(zero_threshold * len(data_list))
    data_zero_count = data_list.count(0)
    if data_zero_count >= new_zero_threshold:
        is_zero = True
        print ("- Check Zero Data(%s/%s) >(%s)" % (data_zero_count, len(data_list), zero_threshold))
    return is_zero, data_zero_count


def main(dir_name, points):
    file_name_list = read_file_by_dir(dir_name)
    max_data_length = 360
    subdata_length = 120
    zero_threshold = 0.003  # >= 1 zero
    count = 0
    error_count = 0
    zero_count1 = 0
    zero_count2 = 0
    zero_count3 = 0
    trend_count1 = {}
    trend_count2 = {}
    trend_count3 = {}
    d = 0
    for i in range(5):
        trend_count[i] = 0
    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        try:
            data_output = read_file(dir_name, file_name)
            data_list = data_output["data"]

            # generate new data by different granularity
            new_data_list = generate_data_by_points(data_list, points)
            check_data_length(new_data_list, max_data_length)

            # split data into different chunks
            chunked_data_list = get_chunk_data_list(data_list, max_data_length)
            for i in range(len(chunked_data_list)):
                print ("\n", count, "- File Name=", file_name)
                chunked_data = chunked_data_list[i]
                check_data_length(chunked_data, max_data_length)
                is_zero, zero_count = discrete_data(chunked_data, zero_threshold)
                if is_zero:
                    new_dir_name = "%s-%sp-zero" % (dir_name, max_data_length)
                    is_zero1, zero1 = discrete_data(chunked_data[:subdata_length], 3*zero_threshold)
                    is_zero2, zero2 = discrete_data(chunked_data[subdata_length:2*subdata_length], 3*zero_threshold)
                    is_zero3, zero3 = discrete_data(chunked_data[2*subdata_length:], 3*zero_threshold)
                    if is_zero1:
                        zero_count1 += 1
                    if is_zero2:
                        zero_count2 += 1
                    if is_zero3:
                        zero_count3 += 1
                    new_file_name = "%s-part%s-%sz-%sz-%sz" % (file_name.split(".")[0], i, zero1, zero2, zero3)
                    write_data_info(new_dir_name, new_file_name, points, chunked_data)
                else:
                    d1 = check_data_trend(chunked_data[:subdata_length])
                    d2 = check_data_trend(chunked_data[subdata_length:2*subdata_length])
                    d3 = check_data_trend(chunked_data[2*subdata_length:3*subdata_length])
                    trend_count1[d1] += 1
                    trend_count2[d2] += 1
                    trend_count3[d3] += 1
                    new_dir_name = "%s-%sp" % (dir_name, max_data_length)
                    new_file_name = "%s-part%s" % (file_name.split(".")[0], i)
                    write_data_info(new_dir_name, new_file_name, points, chunked_data)
                # get_picture(new_data, "workload_picture", file_name)
                count += 1
            print ("\n")
        except Exception as e:
            print ("- Error for %s: %s" % (file_name, str(e)))
            error_count += 1
            pass

        print ("Total=", count, "errors=", error_count, "trend=", trend_count, "zero=", zero_count, "\n")


if __name__ == "__main__":
    try:
        dir_name = sys.argv[1]
        points = int(sys.argv[2])
        main(dir_name, points)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python convert_data_by_granularity.py")
        print ("- dir_name: <source_data>")
        print ("- data points: 60, 3600, 21600, or 86400")
