import connexion
import yaml
from connexion import NoContent
import requests
import logging
import logging.config
import datetime
import json
from pykafka import KafkaClient


STORAGE_URL = "http://localhost:8090"
headers = {"Content-Type": "application/json"}

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def report_food_order(body):
    logger.info("Received event Food Order ID: " + str(body['customer_id']))

    # response = requests.post(app_config["eventstore1"]["url"], json=body, headers=headers)
    #
    # logger.info("INFO " + str(body['customer_id']) + " " + str(response.status_code))

    client = KafkaClient(hosts=app_config["events"]["hostname"] + ":" + str(app_config["events"]["port"]))
    topic = client.topics[str.encode(app_config["events"]["topic"])]
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

    client = KafkaClient(hosts=app_config["events"]["hostname"] + ":" + str(app_config["events"]["port"]))
    topic = client.topics[str.encode(app_config["events"]["topic"])]
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
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    app.run(port=8080)
