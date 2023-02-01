import os
import sys
import time
import json
import math
import shutil
import numpy as np
import pandas as pd
import statistics
import logging
from statsmodels.tsa.seasonal import STL
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.x13 import x13_arima_select_order, _find_x12, x13_arima_analysis
from scipy import signal
from define import metrics_path, main_app_dir, obs_data_length
from collect_data_by_seasonality_changepoint import get_spectrogram
from statsmodels.tsa.stattools import acf, pacf
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
logger = logging.getLogger()


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


def get_picture(data, dir_name, file_name, seasonality):
    start = 0
    x = np.arange(len(data))
    plt.figure(figsize=(8, 4))
    plt.plot(x, data, label="data", color="k", linewidth=1)
    if seasonality != 0:
        plt.axvline(seasonality, label="acfstl", color="r", linestyle=":", linewidth=1)
    ylim = max(data)*1.5
    title_name = "%s" % (file_name)
    plt.title("%s" % title_name)
    plt.ylim(0, ylim)
    plt.legend(loc=0)
    plt.savefig("%s/%s.png" % (dir_name, file_name))
    plt.close()


class STLDecompose:
    data_length = obs_data_length
    end_index = int(data_length/2)

    def __init__(self, new_end_length=0):
        if new_end_length != 0:
            self.end_length = new_end_length

    def get_fs_by_decompose(self, test_array):
        # use STL to find the seasonality with the strongest seasonality
        # https://otexts.com/fpp3/stlfeatures.html
        # Fs = max(0,1,Var(Rt)/Var(St+Rt)
        # Fs---> 0: no seasonality; Fs--->1: strong seasonality
        seasonality = 0
        Fs_list = {}
        start_index = 2
        end_index = self.end_index
        for index in range(end_index):
            if index > 2:
                decompose_result = STL(test_array, robust=True, period=index, seasonal_deg=0, trend_deg=1, low_pass_deg=0).fit()
                Fs = np.max([0.0, 1.0 - np.var(decompose_result.resid) / np.var(decompose_result.seasonal + decompose_result.resid)])
                Fs_list[index] = Fs
        # print ("- FS=", Fs_list)
        return Fs_list

    def get_best_seasonality_by_Fs(self, Fs_list):
        seasonality = 0
        if Fs_list:
            max_Fs = max(Fs_list.values())
            for Fs_index in Fs_list.keys():
                Fs = Fs_list[Fs_index]
                if Fs == max_Fs:
                    seasonality = Fs_index
                    break
        if max_Fs < 0.9:
            seasonality = 0
        return seasonality, max_Fs

    def get_seasonality_by_stl(self, dir_name, file_name):
        print ("- Get Seasonality by STL")
        is_seasonality = False
        data_length = self.data_length
        test_data = read_file(dir_name, file_name).get("data")[:data_length]
        test_array = np.array(test_data)
        Fs_list = self.get_fs_by_decompose(test_array)
        seasonality, max_Fs = self.get_best_seasonality_by_Fs(Fs_list)
        if seasonality != 0:
            is_seasonality = True
        print ("- STL Seasonality = ", seasonality, "Seasonal=", is_seasonality, "MaxFs=", max_Fs)
        return seasonality, is_seasonality, Fs_list


class X13:
    data_length = obs_data_length

    def convert_list_to_pandas(self, list_data):
        print ("- Convert List to PD.series")
        dict_data = {}
        time_data = list(pd.date_range('1/1/2021', freq='M', periods=(len(list_data))))
        for i in range(len(list_data)):
            timestamp = time_data[i]
            dict_data[timestamp] = list_data[i]
        pd_data = pd.Series(dict_data, name="Cost")
        return pd_data

    def get_seasonality_by_x13(dir_name, file_name):
        # https://www.statsmodels.org/dev/generated/statsmodels.tsa.x13.x13_arima_select_order.html
        is_seasonality = False
        try:
            data_length = self.data_length
            test_data = read_file(dir_name, file_name).get("data")[:data_length]
            X13PATH = "/usr/bin/"
            print ("- X12path=", _find_x12(x12path=X13PATH))
            pd_data = self.convert_list_to_pandas(test_data)
            res = x13_arima_select_order(pd_data, x12path=X13PATH)
            trend_order = res["order"]
            seasonal_order = res["sorder"]
            result = res["results"]
            print ("- Order = ", trend_order, seasonal_order)
            if seasonal_order != (0, 0, 0):
                # print (result)
                is_seasonality = True
        except Exception as e:
            print ("- Failed to get seasonality by X13")
            is_seasonality = False
        print ("- X13 Seasonal=", is_seasonality)
        return is_seasonality


class ACFPACF:
    data_length = obs_data_length

    def get_confidence_index_list(self, cf_list, conf_list, conf=0):
        cf_conf_list = {}
        conf1 = conf*(-1)
        conf2 = conf
        for i in range(len(cf_list)):
            cf = cf_list[i]
            # conf1 = conf_list[i][0]  # negative confidence
            # conf2 = conf_list[i][1]  # postiave confidence
            # valid data is >= the confidence value
            # print (i, cf, conf1, cf < conf1, conf2, cf > conf2)
            if cf < conf1 or cf > conf2:
                cf_conf_list[i] = cf
        return cf_conf_list

    def find_max_index(self, cf_conf_list):
        cf_index_list = []
        max_cf = 0
        min_cf = 0
        # find max value exclude index = 0 or 1
        for cf_index in cf_conf_list.keys():
            cf = cf_conf_list[cf_index]
            if cf_index > 2:
                if cf >= max_cf:
                    max_cf = cf
                if cf <= min_cf:
                    min_cf = cf

        for cf_index in cf_conf_list.keys():
            cf = cf_conf_list[cf_index]
            if cf == max_cf or cf == min_cf:
                cf_index_list.append(cf_index)
        return cf_index_list

    def check_pacf_interval(self, acf_index_list, pacf_conf_list):
        pacf_peak_count = {}
        max_length = int(self.data_length/2)
        for acf_index in acf_index_list:
            pacf_peak_count[acf_index] = 0
            index_length = int(max_length/acf_index)
            for index in range(index_length):
                last_pacf = abs(pacf_conf_list[index - 1])
                pacf = abs(pacf_conf_list[index])
                next_pacf = abs(pacf_conf_list[index - 1])
                if pacf > last_pacf and pacf > next_pacf:
                    pacf_peak_count[acf_index] += 1
        return pacf_peak_count

    def find_best_seasonality(self, pacf_peak_count):
        # find best seasonality with high frequency and min. index
        max_pacf_count = max(list(pacf_peak_count.values()))
        best_seasonality = 0
        for pacf_peak in pacf_peak_count:
            pacf_count = pacf_peak_count[pacf_peak]
            if pacf_count == max_pacf_count:
                best_seasonality = pacf_peak
                break
        if best_seasonality < self.data_length/2 and max_pacf_count == 1:
            best_seasonality = 0
        return best_seasonality

    def get_seasonality_by_acf(self, dir_name, file_name):
        print ("- Get Seasonality by ACF/PACF")
        # seasonality in ACF is the index with the max. peak: i.e. 10th acf is the peak
        # seasonality in PACF will repeate the peak index: 10th, 20th, 30th, 40th, ...

        is_seasonality = False
        seasonality = 0
        data_length = self.data_length
        test_data = read_file(dir_name, file_name).get("data")[:data_length]
        test_array = np.array(test_data)
        lags = int(data_length/2) - 1
        try:
            acf_list, aconf_list = acf(test_array, alpha=.05, nlags=lags)
            pacf_list, pconf_list = pacf(test_array, alpha=.05, nlags=lags)
            acf_conf_list = self.get_confidence_index_list(acf_list, aconf_list)
            pacf_conf_list = self.get_confidence_index_list(pacf_list, pconf_list)
            acf_index_list = self.find_max_index(acf_conf_list)
            print ("- ACF Index=", acf_index_list)
            pacf_peak_count = self.check_pacf_interval(acf_index_list, list(pacf_list))
            print ("- PACF =", pacf_peak_count)
            seasonality = self.find_best_seasonality(pacf_peak_count)
            if acf_index_list and seasonality > 0:
                is_seasonality = True
        except Exception as e:
            print ("Failed to get Seasonality by ACF/PACF: %s" % str(e))
        print ("- ACF/PACF Seasonality = ", seasonality, "Seasonal=", is_seasonality)
        return seasonality, is_seasonality


def main(dir_name, cluster_name):
    file_name_list = read_file_by_dir(dir_name)
    count = 0
    error_count = 0
    error_count = 0
    seasonality_count = 0
    x13_count = 0
    acf_count = 0
    stl_count = 0
    no_seasonality_count = 0
    seasonality = 0
    for file_name in file_name_list:
        print (count, "- File Name=", file_name)
        try:
            data = read_file(dir_name, file_name).get("data")[:obs_data_length]
            # x13_is_seasonality = get_seasonality_by_x13(dir_name, file_name)
            acf_seasonality, acf_is_seasonality = ACFPACF().get_seasonality_by_acf(dir_name, file_name)
            stl_seasonality, stl_is_seasonality, Fs_list = STLDecompose().get_seasonality_by_stl(dir_name, file_name)
            if acf_is_seasonality:
                acf_count += 1
            if stl_is_seasonality:
                stl_count += 1

            new_file_name = "%s-acfstl=%s" % (file_name, seasonality)
            if ((acf_is_seasonality and stl_is_seasonality) and (acf_seasonality == stl_seasonality)) or (acf_is_seasonality and Fs_list.get(acf_seasonality) > 0.99):
                print ("- Seasonality due to ACF = STL or FC of ACF>0.99")
                seasonality_count += 1
                seasonality = acf_seasonality
                get_picture(data, "with_seasonality_picture", new_file_name, seasonality)
            else:
                no_seasonality_count += 1

                get_picture(data, "without_seasonality_picture", new_file_name, seasonality)
            print ("\n")
        except Exception as e:
            print ("- Error=", file_name, str(e))
            error_count += 1
            pass
        count += 1
        print ("Total=", count, "errors=", error_count)
        print ("ACF=", acf_count, "STL=", stl_count)
        print ("Seasonality=", seasonality_count, "NoSeasonality=", no_seasonality_count)
        print ("\n")


if __name__ == "__main__":
    try:
        dir_name = sys.argv[1]
        cluster_name = sys.argv[2]
        main(dir_name, cluster_name)
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python compare_data_by_crosscorrelation_seasonality.py")
        print (" - dir_name: <dir_name>")
        print (" - cluster_name: <cluster_name>")
