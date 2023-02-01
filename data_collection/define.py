# configuration
traffic_path = "./traffic"
metrics_path = "./metrics"
picture_path = "./picture"
config_path = "./config"
pattern_path = "./pattern"

# data
main_app_dir = "total_metrics-azure-d01-d14-short"
main_app_name = "d01-d14-49d84-94409-8203f-queue_output.json"
# main_app_dir = "total_metrics-alibaba-app-cpu-mem-rtq"
# main_app_name = "alibaba-cpu-MSResource_0.csv-315f624d255fc7680cb985a3e0796397bc18baa89d4226331ebeae5a62de747f-cpu_output.json"
obs_data_start_index = 0
obs_data_length = 120
pred_data_length = 30
granularity = 120
zero_count = 60

# log
log_interval = 30  # collect data per 30 seconds
query_interval = 1
query_timeout = log_interval