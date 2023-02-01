import os
import sys
import time
import json
import math
import shutil
import numpy as np
import statistics
import logging
import burst_detection as bd
from scipy import signal
from statsmodels.tsa.seasonal import STL
import ruptures as rpt
from define import metrics_path, main_app_dir, obs_data_length
from neurodsp.burst import detect_bursts_dual_threshold, compute_burst_stats
from statsmodels.stats.stattools import durbin_watson


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
    detailed_file_name = "%s/%s" % (dir_name, file_name)
    try:
        with open(detailed_file_name) as file:
            data_output = json.load(file)
    except Exception as e:
        print ("failed to read file(%s): %s" % (detailed_file_name, str(e)))
    return data_output


def get_normalize(Sxx_list):
    # print ("- Get Normalize", Sxx_list)
    min_sxx = min(Sxx_list)
    max_sxx = max(Sxx_list)
    new_Sxx_list = []
    for sxx in Sxx_list:
        new_sxx = (sxx - min_sxx)/(max_sxx - min_sxx)
        new_Sxx_list.append(new_sxx)
    # print ("- Get Normalize list =", new_Sxx_list)
    return new_Sxx_list


def convert_2darray_to_1dlist(Sxx):
    print ("- Convert 2darray")
    Sxx_list = []
    for sxx_array in Sxx.tolist():
        sxx = sxx_array[0]
        Sxx_list.append(sxx)
    # print ("- Convert Sxx list = ", Sxx_list)
    return Sxx_list


def get_spectrogram(test_data):
    print ("- Get Spectrogram")
    test_array = np.array(test_data)
    f, t, Sxx = signal.spectrogram(test_array)
    Sxx_list = convert_2darray_to_1dlist(Sxx)
    max_orig_sxx = max(Sxx_list)
    max_sxx_index = 0
    if max_orig_sxx > 0.5:
        new_Sxx = get_normalize(Sxx_list)
        max_sxx = max(new_Sxx)
        max_sxx_index = new_Sxx.index(max_sxx)
        print ("- Find Best Index = ", max_sxx_index, "Best Strength =", max_sxx, max_orig_sxx)
    return max_sxx_index


def get_seasonality(dir_name, file_name):
    print ("- Get seasonality")
    is_seasonality = False
    data_length = obs_data_length
    test_data = read_file(dir_name, file_name).get("data")[:data_length]
    test_array = np.array(test_data)
    max_sxx_index = get_spectrogram(test_data)
    if max_sxx_index >= 2:
        decompose_result = STL(test_array, robust=True, period=max_sxx_index, seasonal_deg=0, trend_deg=1, low_pass_deg=0).fit()
        seasonal = list(decompose_result.seasonal)
        if seasonal:
            is_seasonality = True
    return is_seasonality


def get_change_point(dir_name, file_name):
    print ("- Get Change Point")
    data_length = obs_data_length
    is_changepoint = False
    start = 0
    end = start + data_length
    part1_count = 0
    part2_count = 0
    part3_count = 0
    test_data = read_file(dir_name, file_name).get("data")[start:end]
    total_mean = np.mean(test_data)
    total_stdev = np.std(np.array(test_data), ddof=1)
    threshold = 1*total_stdev
    test_array = np.array(test_data)
    window = 30
    window_length = int(obs_data_length/window) - 1
    changepoint_list = []

    # https://centre-borelli.github.io/ruptures-docs/
    # import ruptures as rpt
    # algo = rpt.Dynp(model="l2", jump=1, min_size=2).fit(test_array)
    algo = rpt.KernelCPD(kernel="linear", min_size=2).fit(test_array)
    result = algo.predict(n_bkps=window_length)

    print ("- Get Change Point =", len(result), result)
    for index in result:
        last_window_mean = np.mean(test_data[index-window:index])
        next_window_mean = np.mean(test_data[index:index+window])
        window_diff = abs(next_window_mean - last_window_mean)
        if index < 120 and window_diff >= threshold:
            part1_count += 1
        elif index >= 120 and index < 240 and window_diff >= threshold:
            part2_count += 1
        elif index >= 240 and index < 360 and window_diff >= threshold:
            part3_count += 1
        else:
            index = -1
        if index != -1:
            changepoint_list.append(index)
    if part1_count > 0:
        is_changepoint = True
        print ("- Find Part 1 Bkps = ", part1_count, "Part 2 Bkps =", part2_count, "Part 3 Bkps =", part3_count)
    return is_changepoint, part1_count, part2_count, part3_count, changepoint_list


def detect_burst(test_data):
    print ("- Detect Burst ")
    # from neurodsp.burst import detect_bursts_dual_threshold
    test_array = np.array(test_data)
    window = 30
    amp_dual_thresh = (1, 2)
    f_range = (8, 12)
    burst_data = detect_bursts_dual_threshold(test_array, window, amp_dual_thresh, f_range)
    # print ("- Detect Burst=", burst_data)
    return burst_data


def find_bursts(dir_name, file_name):
    print ("- Get Burst")
    data_length = obs_data_length
    is_events = False
    start = 0
    end = start + data_length
    test_data = read_file(dir_name, file_name).get("data")[start:end]
    total_mean = np.mean(test_data)
    total_stdev = np.std(np.array(test_data), ddof=1)
    threshold = 3*total_stdev

    part1_count = 0
    part2_count = 0
    part3_count = 0
    test_array = np.array(test_data, dtype=float)

    burst_data = detect_burst(test_data)
    for index in range(len(burst_data)):
        is_burst = burst_data[index]
        burst_value = test_data[index]
        burst_diff = abs(burst_value - total_mean)
        if is_burst and burst_diff >= threshold:
            if index < 120:
                part1_count += 1
            if index >= 120 and index < 240:
                part2_count += 1
            if index < 360 and index <= 240:
                part3_count += 1
            if part1_count > 0:
                is_events = True
    print ("- Find Part 1 Bursts = ", part1_count, "Part 2 Bursts =", part2_count, "Part 3 Bursts =", part3_count)
    return is_events, part1_count, part2_count, part3_count


def detect_linear_trend(dir_name, file_name):
    print ("- Detect Linear Trend")
    data_length = obs_data_length
    is_linear_trend = False
    start = 0
    end = start + data_length
    test_data = read_file(dir_name, file_name).get("data")[start:end]

    # from scipy import signal
    test_array = np.array(test_data, dtype=float)
    detrend_result1 = signal.detrend(test_array, type="linear")
    test_result1 = durbin_watson(detrend_result1)

    detrend_result2 = signal.detrend(test_array, type="constant")
    test_result2 = durbin_watson(detrend_result2)

    # no correlation between residuals when the model is linear
    # test_result = 2: no serial correlation
    # test_result < 2: postive serial correlation
    # test_result > 2: negative serial correlation

    if test_result1 == 2 or test_result2 == 2:
        is_linear_trend = True
    return is_linear_trend


def main(dir_name, cluster_name):

    file_name_list = read_file_by_dir(dir_name)
    count = 0
    others_count = 0
    error_count = 0
    linear_trend_count = 0
    no_linear_trend_count = 0
    seasonality_count = 0
    no_seasonality_count = 0
    changepoint_count = 0
    no_changepoint_count = 0
    event_count = 0
    no_event_count = 0

    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        try:
            is_linear_trend = detect_linear_trend(dir_name, file_name)
            is_seasonality = get_seasonality(dir_name, file_name)
            is_changepoint, part1_count, part2_count, part3_count = get_change_point(dir_name, file_name)
            is_events, part1_count, part2_count, part3_count = find_bursts(dir_name, file_name)

            # new_file_name = "%s-part1=%s-part2=%s-part3=%s" % (file_name, part1_count, part2_count, part3_count)
            # if is_seasonality:
            #     shutil.copyfile("%s/%s" % (dir_name, file_name), "./with_seasonality/%s" % (new_file_name))
            # else:
            #     shutil.copyfile("%s/%s" % (dir_name, file_name), "./without_seasonality/%s" % (new_file_name))

            if is_linear_trend:
                linear_trend_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./with_linear_trend/%s" % (file_name))
            else:
                no_linear_trend_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./without_linear_trend/%s" % (file_name))

            if is_seasonality:
                seasonality_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./with_seasonality/%s" % (file_name))
            else:
                no_seasonality_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./without_seasonality/%s" % (file_name))
            if is_changepoint:
                changepoint_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./with_changepoint/%s" % (file_name))
            else:
                no_changepoint_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./without_changepoint/%s" % (file_name))
            if is_events:
                event_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./with_event/%s" % (file_name))
            else:
                no_event_count += 1
                # shutil.copyfile("%s/%s" % (dir_name, file_name), "./without_event/%s" % (file_name))

            print ("\n")
        except Exception as e:
            print ("- Error=", file_name, str(e))
            error_count += 1
            pass
        count += 1
        print ("Total=", count, "errors=", error_count, "linear_trend =", linear_trend_count, "no_linear_trend =", no_linear_trend_count, "seasonality = ", seasonality_count, "no_seasonality = ", no_seasonality_count, "change_point = ", changepoint_count, "no_change_point = ", no_changepoint_count, "event = ", event_count, "no_event = ", no_event_count)
        print ("\n")


if __name__ == "__main__":
    try:
        dir_name = sys.argv[1]
        cluster_name = sys.argv[2]
        main(dir_name, cluster_name)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python collect_data_by_seasonal_decompose.py")
        print (" - dir_name: <dir_name>")
        print (" - cluster_name: <cluster_name>")
