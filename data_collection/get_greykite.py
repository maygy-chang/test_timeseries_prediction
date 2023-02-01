import json
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings("ignore")
from greykite.framework.templates.autogen.forecast_config import ForecastConfig
from greykite.framework.templates.autogen.forecast_config import MetadataParam
from greykite.framework.templates.forecaster import Forecaster
from greykite.framework.templates.model_templates import ModelTemplateEnum
# reference: https://linkedin.github.io/greykite/docs/0.1.0/html/gallery/quickstart/0100_simple_forecast.html


class Greykite:
    start_index = 120
    step = 30

    def __init__(self):
        pass

    def convert_dict_to_pandas(self, dict_data):
        pd_data = pd.DataFrame(list(dict_data.items()), columns=['ts','y'])
        # print (pd_data.head())
        return pd_data

    def convert_list_to_pandas(self, list_data):
        pd_data = pd.DataFrame()
        pd_data["ds"] = pd.date_range('1/1/2021', freq='D', periods=(len(list_data)))
        pd_data["y"] = np.array(list_data)
        return pd_data

    def do_prediction(self, pd_data, step):
        metadata = MetadataParam(time_col="ds", value_col="y", freq="D")
        f = Forecaster()
        result = f.run_forecast_config(
            df=pd_data, 
            config=ForecastConfig(
                model_template=ModelTemplateEnum.SILVERKITE.name,
                forecast_horizon=30,  # forecasts 365 steps ahead
                coverage=0.95,         # 95% prediction intervals
                metadata_param=metadata
            )
        )
        pred_data_list = list(result.forecast.df.forecast[self.start_index:self.start_index+step])
        return pred_data_list 


def read_file(file_name):
    data_output = {}
    try:
        with open(file_name) as file:
            data_output = json.load(file) 
    except Exception as e:
        print ("failed to read file(%s): %s" % (file_name, str(e)))
    return data_output


def main():
    file_name = "./datadog-monitoring-cluster-agent-kube_replicaset_status_ready_replicas_output.json"
    data = read_file(file_name)
    g = Greykite()
    obs_data = g.convert_list_to_pandas(data["data"][:120])
    result = g.do_prediction(obs_data, 30)
    print (result)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print ("Exception:")
        print ("- %s" % str(e))
        print ("python get_greykite.py")

