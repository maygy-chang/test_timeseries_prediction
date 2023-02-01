import json
import numpy as np
import pandas as pd
from fbprophet import Prophet


class FBProphet:
    p = Prophet()

    def __init__(self):
        pass

    def do_prediction(self, pd_data, step):
        print ("- Do Prediction")
        p = Prophet(yearly_seasonality=True,daily_seasonality=True)
        p.fit(pd_data)
        future = p.make_future_dataframe(periods=step)
        forecast = p.predict(future)
        # print (forecast.tail())
        pred_data_list = (list(forecast["yhat"]))
        return pred_data_list

    def convert_dict_to_pandas(self, dict_data):
        pd_data = pd.DataFrame(list(dict_data.items()), columns=['ds','y'])
        # print (pd_data.head())
        return pd_data 

    def convert_list_to_pandas(self, list_data):
        print ("- Convert Data")
        pd_data = pd.DataFrame()
        pd_data["ds"] = pd.date_range('1/1/2021', freq='D', periods=(len(list_data)))
        pd_data["y"] = np.array(list_data)
        return pd_data


def read_file(file_name):
    data_output = {}
    try:
        with open(file_name) as file:
            data_output = json.load(file)
    except Exception as e:
        print ("failed to read file(%s): %s" % (file_name, str(e)))
    return data_output


def main():
    file_name = "./total_metrics/172-31-6-30-strimzi-cluster-operator-pod_memory_working_set_bytes_output.json"
    data = read_file(file_name)
    f = FBProphet()
    obs_data = f.convert_list_to_pandas(data["data"][:120])    
    pred_data = f.do_prediction(obs_data, 30)
    print (pred_data, len(pred_data))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python get_prophet.py")

