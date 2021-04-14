import connexion
import yaml
from connexion import NoContent
import requests
import logging
import logging.config
import datetime
import json
from pykafka import KafkaClient
import os
import time

STORAGE_URL = "http://localhost:8090"
headers = {"Content-Type": "application/json"}

if "TARGET_ENV" in os.environ and os.environ["TARGET_ENV"] == "test":
    print("In Test Environment")
    app_conf_file = "/config/app_conf.yml"
    log_conf_file = "/config/log_conf.yml"
else:
    print("In Dev Environment")
    app_conf_file = "app_conf.yml"
    log_conf_file = "log_conf.yml"
with open(app_conf_file, 'r') as f:
    app_config = yaml.safe_load(f.read())

# External Logging Configuration
with open(log_conf_file, 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')

logger.info("App Conf File: %s" % app_conf_file)
logger.info("Log Conf File: %s" % log_conf_file)

max_retry = int(app_config["events"]["max_retry"])
retry_count = 0

while retry_count < max_retry:
    try:
        logger.info("Trying to Connect to Kafka " + str(retry_count))
        client = KafkaClient(hosts=app_config["events"]["hostname"] + ":" + str(app_config["events"]["port"]))
        topic = client.topics[str.encode(app_config["events"]["topic"])]
        retry_count = max_retry
    except:
        logger.error("Connecting to Kafka failed " + str(retry_count))
        time.sleep(app_config["events"]["sleep"])
        retry_count += 1


def report_food_order(body):
    logger.info("Received event Food Order ID: " + str(body['customer_id']))

    # response = requests.post(app_config["eventstore1"]["url"], json=body, headers=headers)
    #
    # logger.info("INFO " + str(body['customer_id']) + " " + str(response.status_code))

    # client = KafkaClient(hosts=app_config["events"]["hostname"] + ":" + str(app_config["events"]["port"]))
    # topic = client.topics[str.encode(app_config["events"]["topic"])]
    producer = topic.get_sync_producer()
    msg = {"type": "fo",
           "datetime":
               datetime.datetime.now().strftime(
                   "%Y-%m-%d %H:%M:%S.%f"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("INFO " + str(body['customer_id']))

    # logger.info("Returned event Food Order ID: " + str(body['customer_id']))

    return NoContent, 201


def report_scheduled_order(body):
    logger.info("INFO " + str(body['customer_id']))

    # response = requests.post(app_config["eventstore2"]["url"], json=body, headers=headers)
    #
    # logger.info("INFO " + str(body['customer_id']) + " " + str(response.status_code))

    producer = topic.get_sync_producer()
    msg = {"type": "so",
           "datetime":
               datetime.datetime.now().strftime(
                   "%Y-%m-%d %H:%M:%S.%f"),
           "payload": body}
    msg_str = json.dumps(msg)
    producer.produce(msg_str.encode('utf-8'))
    logger.info("INFO " + str(body['customer_id']))

    return NoContent, 201


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", base_path="/receiver", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    app.run(port=8080)
