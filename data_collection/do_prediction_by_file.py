import os
import sys
import time
import json
import math
import numpy as np
import pandas as pd
import psutil
from get_sarima import SARIMA
from get_greykite import Greykite
from get_prophet import FBProphet
from define import metrics_path, main_app_name, main_app_dir


def count_mape(obs_data, pred_data):
    print ("- Count MAPE")
    negative_count = 0
    mape_list = []
    avg_mape = 0
    for i in range(len(obs_data)):
        obs = obs_data[i]
        pred = pred_data[i]
        if pred < 0:
            negative_count += 1
        if obs == 0:
            obs = 1
        mape = abs(obs-pred)/obs*100
        mape_list.append(mape)
    if len(mape_list) != 0:
        avg_mape = sum(mape_list)/len(mape_list)
    print ("- Avg. MAPE:", avg_mape, negative_count)
    return avg_mape


def read_file_by_dir(dir_name, file_num):
    count = 0
    file_name_list = []
    file_list = os.listdir(dir_name)
    for file_name in sorted(file_list):
        file_name_list.append(file_name)
        count += 1
        if count >= file_num:
            break
    print ("- Get %s files in Dir: %s" % (file_num, dir_name))
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


def get_process_cpu():
    process_cpu_percent = 0
    try:
        cmd = "ps -eo pid,ppid,cmd,%cpu | grep python3.8 | grep do_prediction"
        output = os.popen(cmd).read()
        print ("Process: ", output)
        process_cpu_percent = float(output.split()[-1])
        print ("Get Process CPU=", process_cpu_percent)
    except Exception as e:
        print ("Failed to get process cpu: %s" % str(e))
    return process_cpu_percent


def main(algo_name, dir_name, file_num):
    file_name_list = read_file_by_dir(dir_name, file_num)
    data_length = 120
    step = 30
    total_mape = 0
    start_time = time.time()
    output = ""
    count = 0
    nan_count = 0
    small_mape = 0
    large_mape = 0
    small_mape_count = 0
    large_mape_count = 0
    error_count = 0
    mape_list = []
    time_list = []
    cpu_list = []

    var_count = 0
    var_time = 0
    tf_count = 0
    tf_time = 0
    sarima_count = 0
    sarima_time = 0
    var_duration = 0
    tf_duration = 0
    sarima_duration = 0
    total_algo_diff = 0
    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        output = ""
        data_output = read_file(dir_name, file_name)
        data = data_output["data"]
        start_time1 = time.time()
        mape = 0
        algo_diff = 0
        try:
            if algo_name == "sarima":
                s = SARIMA()
                pred_data = s.do_prediction_by_sarima(data[:data_length], step)
                mape = count_mape(data[data_length:data_length+step], pred_data)

            elif algo_name == "greykite":
                g = Greykite()
                obs_data = g.convert_list_to_pandas(data[:data_length])
                pred_data = g.do_prediction(obs_data, step)
                mape = count_mape(data[data_length:data_length+step], pred_data)

            elif algo_name == "fbprophet":
                f = FBProphet()
                obs_data = f.convert_list_to_pandas(data[:data_length])
                pred_data = f.do_prediction(obs_data, step)
                # print (pred_data, len(pred_data), len(data[data_length:data_length+step]), len(data), data)
                mape = count_mape(data[data_length:data_length+step], pred_data)
        except Exception as e:
            print ("Failed to prediction: %s" % str(e))
            error_count += 1
            pass

        end_time1 = time.time()
        diff_time1 = end_time1 - start_time1
        # error count
        if not np.isnan(mape):
            total_mape += mape
        else:
            nan_count += 1
        if mape > 100:
            large_mape_count += 1
            large_mape += mape
        else:
            small_mape_count += 1
            small_mape += mape

        mape_list.append(mape)
        time_list.append(diff_time1)
        process_cpu = get_process_cpu()
        cpu_list.append(process_cpu)
        print ("- Index=%s, Algo=%s, File_name=%s, MAPE=%s, Diff_Time=%s Error=%s, CPU=%s\n" % (count, algo_name, file_name, mape, diff_time1, error_count, process_cpu))
        output += "Index=%s, Algo=%s, File_name=%s, MAPE=%s, Diff_Time=%s Error=%s, CPU=%s" % (count, algo_name, file_name, mape, diff_time1, error_count, process_cpu)
        write_prediction_info(algo_name, output, file_num, dir_name)
        count += 1
        print ("\n")

    end_time = time.time()
    diff_time = end_time - start_time
    avg_mape = total_mape/len(file_name_list)
    time_p95 = np.percentile(np.array(time_list), 95)
    mape_p95 = np.percentile(np.array(mape_list), 95)
    avg_cpu = sum(cpu_list)/len(cpu_list)
    avg_large_mape = 0
    if large_mape_count != 0:
        avg_large_mape = large_mape/large_mape_count
    avg_small_mape = 0
    if small_mape_count != 0:
        avg_small_mape = small_mape/small_mape_count
    print ("- Total_Index=%s, Algo=%s, Total_MAPE=%s, Dir_name=%s, Total_Diff_Time=%s, NAN=%s, MAPE>100=%s/%s, MAPE<=100=%s/%s Error=%s, P95time=%s P95MAPE=%s, CPU=%s\n" % (count, algo_name, avg_mape, dir_name, diff_time, nan_count, large_mape_count, avg_large_mape, small_mape_count, avg_small_mape, error_count, time_p95, mape_p95, avg_cpu))
    output += "Total_Index=%s, Algo=%s, Total_MAPE=%s, Dir_name=%s, Total_Diff_Time=%s, NAN=%s, MAPE>100=%s/%s, MAPE<=100=%s/%s Error=%s P95time=%s P95MAPE=%s CPU=%s" % (count, algo_name, avg_mape, dir_name, diff_time, nan_count, large_mape_count, avg_large_mape, small_mape_count, avg_small_mape, error_count, time_p95, mape_p95, avg_cpu)
    write_prediction_info(algo_name, output, file_num, dir_name)


def write_prediction_info(algo_name, output, file_num, dir_name):
    file_name = "%s/%s_%s_prediction_result%s" % (metrics_path, dir_name, algo_name, file_num)
    try:
        with open(file_name, "a") as f:
            line = "%s \n" % (output)
            f.write(line)
    except Exception as e:
        print ("failed to write scale info: %s" % str(e))


if __name__ == "__main__":
    try:
        algo_name = sys.argv[1]
        dir_name = sys.argv[2]
        file_num = int(sys.argv[3])
        main(algo_name, dir_name, file_num)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python do_prediction_by_file.py <algo_name> <dir_name> <file_num>")
        print ("- algo_name:  sarima or grekite")
        print ("- dir_name:  test_dir")
        print ("- file_num:  1000")
