import requests
import json
import re

path_to_json_input_file = "/Users/aaronjacobs/Desktop/ConfluentMonitor/cluster_input.json"
headers = {'Content-type': 'application/json', 'Accept': 'application/json'}

# below metrics from Confluent REST API - https://api.telemetry.confluent.cloud/docs/descriptors/datasets/cloud
metric_names_list_topics = ["io.confluent.kafka.server/received_bytes",
                            "io.confluent.kafka.server/received_records",
                            "io.confluent.kafka.server/retained_bytes",
                            "io.confluent.kafka.server/sent_bytes",
                            "io.confluent.kafka.server/consumer_lag_offsets"]


# https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.schema_registry.id=lsrc-kgndpp
# all the below JSON payloads are taken from the Confluent REST API
json_payload_partition_count = ('{ "aggregations": [{ "metric": "io.confluent.kafka.server/partition_count" }], '
                                '"filter": '
                                '{ "field": '
                                '"resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                                '"granularity": "PT1M", "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')

json_payload_cluster_load_percent = ('{ "aggregations":[{"metric": "io.confluent.kafka.server/cluster_load_percent"}], '
                                     '"filter": '
                                     '{ "field": '
                                     '"resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                                     '"granularity": "PT1M", "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')

json_payload_topic = ('{ "aggregations": [ { "metric": "METRIC_NAME_PLACEHOLDER" } ], '
                      '"filter": '
                      '{ "field": "resource.kafka.id", "op": "EQ", "value": "CLUSTER_NAME_PLACEHOLDER" }, '
                      '"granularity": "PT1M", '
                      '"group_by": [ "metric.topic" ], "intervals": [ "PT1M/now-2m|m" ], "limit": 25 }')


def process_non_topic_metric_cluster_load_percent(string_in, metric_name_in, cluster_name, env, url_in, un, pw):
    new_cluster_str = string_in.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
    d = json.loads(new_cluster_str)
    r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
    response = json.loads(r.text)
    if "data" in response:
        key = response["data"]
        for i in key:
            metric_value = i["value"]
            # multiply by 100 and take the int of this, AppD only accepts positive integer values
            metric_value = int(metric_value * 1000)
            print("name=Custom Metrics|Confluent|{}-Cluster|{}|{}, value={}".format(env, cluster_name, metric_name_in, metric_value))


# total number of schemas is metric he wants, request counts metric also wanted
# schema registry ID - lsrc-3nqrko - this will be different - 1 env can have multiple clusters with single schema registry
# https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.schema_registry.id=lsrc-kgndpp
def process_non_topic_metric(string_in, metric_name_in, cluster_name, env, url_in, un, pw):
    new_cluster_str = string_in.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
    d = json.loads(new_cluster_str)
    # print("cluster name: ", cluster_name)
    # r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
    r = requests.get(url_in, headers=headers, auth=(un, pw))
    response = json.loads(r.text)
    if "data" in response:
        key = response["data"]
        for i in key:
            metric_value = int(i["value"])
            print("name=Custom Metrics|Confluent|{}-Cluster|{}|{}, value={}".format(env, cluster_name, metric_name_in, metric_value))


def process_topic_metrics(metrics_list, json_str, cluster_name, topic_exclusion_list, env, url_in, un, pw):
    for i in metrics_list:
        new_cluster_str = json_str.replace("CLUSTER_NAME_PLACEHOLDER", cluster_name)
        new_metric_str = new_cluster_str.replace("METRIC_NAME_PLACEHOLDER", i)
        # regex to get short metric names from line 16 metric_names_list_topics list
        pattern = r'\/([^" \]}]+)'
        match = re.search(pattern, new_metric_str)
        metric_name = "None"
        if match:
            metric_name = match.group(1)
        d = json.loads(new_metric_str)
        r = requests.post(url_in, headers=headers, json=d, auth=(un, pw))
        response = json.loads(r.text)
        if "data" in response:
            key = response["data"]
            for j in key:
                topic_name = j["metric.topic"]
                if topic_name not in topic_exclusion_list:
                    metric_value = int(j["value"])
                    print("name=Custom Metrics|Confluent|{}-Cluster|{}|Topic|{}|{}, value={}".format(env, cluster_name, topic_name, metric_name, metric_value))
                elif topic_name in topic_exclusion_list:
                    metric_value = int(j["value"])
                    print("name=Custom Metrics|Confluent|{}-Cluster|{}|alt_topics|Topic|{}|{}, value={}".format(env, cluster_name, topic_name, metric_name, metric_value))


def get_schema_metrics():
    url = "https://api.telemetry.confluent.cloud/v2/metrics/cloud/export?resource.schema_registry.id=<CLUSTER_NAME_HERE>"
    headers = {
      'Authorization': 'Basic <AUTH TOKEN HERE>'
    }
    response = requests.request("GET", url, headers=headers)
    resp_text = response.text
    # print(response.text)

    # confluent_kafka_schema_registry_schema_count
    pattern = r'confluent_kafka_schema_registry_schema_count\{[^}]+\} (\d+\.\d+) \d+'
    match = re.search(pattern, resp_text)
    if match:
        schema_count = int(float(match.group(1)))
        print("name=Custom Metrics|Confluent|confluent_kafka_schema_registry_schema_count, value={}".format(schema_count))
    else:
        print("No match found.")

    # confluent_kafka_schema_registry_request_count
    pattern = r'confluent_kafka_schema_registry_request_count\{[^}]+\} (\d+\.\d+) \d+'
    match = re.search(pattern, resp_text)
    if match:
        request_count = int(float(match.group(1)))
        print("name=Custom Metrics|Confluent|confluent_kafka_schema_registry_request_count, value={}".format(request_count))
    else:
        print("No match found.")

    # confluent_kafka_schema_registry_schema_operations_count\{method="READ
    pattern = r'confluent_kafka_schema_registry_schema_operations_count\{method="READ",[^}]+\} (\d+\.\d+) \d+'
    match = re.search(pattern, resp_text)
    if match:
        operations_count_read = int(float(match.group(1)))
        print("name=Custom Metrics|Confluent|confluent_kafka_schema_registry_schema_operations_count-READ, value={}".format(operations_count_read))
    else:
        print("No match found.")


def execute_functions(json_file):
    f = open(json_file)
    data = json.load(f)
    cluster_in_counter = 0
    for confluent_cluster in data:
        element = data[cluster_in_counter]
        username = str(element['username'])
        password = str(element['password'])
        confluent_api_url = str(element['confluent_api_url'])
        confluent_cluster_name = str(element['confluent_cluster_name'])
        environment = str(element['environment'])
        alt_path_topics_list_in = str(element['alt_path_topics_list'])

        # print("un: ", username)
        # print("password: ", password)
        # print("confluent api url: ", confluent_api_url)
        # print("confluent cluster name: ", confluent_cluster_name)
        # print("env: ", environment)
        # print("alt path topics list: ", alt_path_topics_list_in)
        # print()

        process_topic_metrics(metric_names_list_topics, json_payload_topic, confluent_cluster_name, alt_path_topics_list_in, environment, confluent_api_url, username, password)
        process_non_topic_metric(json_payload_partition_count, "partition_count", confluent_cluster_name, environment, confluent_api_url, username, password)
        process_non_topic_metric_cluster_load_percent(json_payload_cluster_load_percent, "cluster_load_1000_percent", confluent_cluster_name, environment, confluent_api_url, username, password)
        # print()
        cluster_in_counter += 1


get_schema_metrics()
execute_functions(path_to_json_input_file)

