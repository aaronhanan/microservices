import connexion
from connexion import NoContent
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from base import Base
from food_order import FoodOrder
from scheduled_order import ScheduledOrder
import yaml
import logging.config
import datetime
from pykafka import KafkaClient
from pykafka.common import OffsetType
from threading import Thread
import json
import os
from sqlalchemy import and_
import time

MAX_EVENTS = 10
EVENT_FILE = 'events.json'
events = []
date_format = '%Y-%m-%d %H:%M:%S.%f'

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


DB_ENGINE = create_engine('mysql+pymysql://' +
                          app_config['datastore']['user'] + ':' +
                          app_config['datastore']['password'] + '@' +
                          app_config['datastore']['hostname'] + ':' +
                          app_config['datastore']['port'] + '/' +
                          app_config['datastore']['db'])
DB_SESSION = sessionmaker(bind=DB_ENGINE)


# Your functions here


def report_food_order(body):
    """ Receives a food delivery order """
    session = DB_SESSION()

    fo = FoodOrder(body['customer_id'],
                   body['name'],
                   body['phone'])

    session.add(fo)
    session.commit()
    session.close()
    logger.debug("report_food_order " + str(body['customer_id']))


def report_scheduled_order(body):
    """ Receives a scheduled delivery order """
    session = DB_SESSION()

    so = ScheduledOrder(body['customer_id'],
                        body['name'],
                        body['phone'],
                        body['scheduled_date'])

    session.add(so)
    session.commit()
    session.close()
    logger.debug("DEBUG: report_scheduled_order " + str(body['customer_id']))


def get_food_order(start_timestamp, end_timestamp):
    """ Gets new food order after the timestamp """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    print(start_timestamp_datetime)

    orders = session.query(FoodOrder).filter(and_(FoodOrder.order_date >= start_timestamp_datetime, FoodOrder.order_date < end_timestamp_datetime))
    results_list = []

    for order in orders:
        results_list.append(order.to_dict())
        print(order.to_dict())

    session.close()

    logger.info("Query for Food Orders after %s returns %d results" % (start_timestamp, len(results_list)))

    return results_list, 200


def get_scheduled_order(start_timestamp, end_timestamp):
    """ Gets new scheduled order after the timestamp """

    session = DB_SESSION()

    start_timestamp_datetime = datetime.datetime.strptime(start_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    end_timestamp_datetime = datetime.datetime.strptime(end_timestamp, "%Y-%m-%d %H:%M:%S.%f")
    # print(start_timestamp_datetime)

    orders = session.query(ScheduledOrder).filter(and_(ScheduledOrder.order_date >= start_timestamp_datetime, ScheduledOrder.order_date < end_timestamp_datetime))
    results_list = []

    for order in orders:
        results_list.append(order.to_dict())

    session.close()

    logger.info("Query for Scheduled Orders after %s returns %d results" % (start_timestamp, len(results_list)))

    return results_list, 200


def process_messages():
    """ Process event messages """
    tmp_max = app_config["events"]["max_retry"]
    retry_count = 0

    hostname = "%s:%d" % (app_config["events"]["hostname"],
                          app_config["events"]["port"])

    while retry_count < tmp_max:
        try:
            logger.info("Trying to Connect to Kafka " + retry_count)

            client = KafkaClient(hosts=hostname)
            topic = client.topics[str.encode(app_config["events"]["topic"])]
        except:
            logger.error("Connecting to Kafka failed")
            time.sleep(app_config["events"]["sleep"])
            retry_count += 1

    # Create a consume on a consumer group, that only reads new messages
    # (uncommitted messages) when the service re-starts (i.e., it doesn't
    # read all the old messages from the history in the message queue).
    consumer = topic.get_simple_consumer(consumer_group=b'event_group',
                                         reset_offset_on_start=False,
                                         auto_offset_reset=OffsetType.LATEST)

    # This is blocking - it will wait for a new message
    for msg in consumer:
        msg_str = msg.value.decode('utf-8')
        msg = json.loads(msg_str)
        logger.info("Message: %s" % msg)

        payload = msg["payload"]

        if msg["type"] == "fo":  # Change this to your event type
            # Store the food order (i.e., the payload) to the DB
            report_food_order(payload)

        elif msg["type"] == "so":  # Change this to your event type
            # Store the scheduled order (i.e., the payload) to the DB
            report_scheduled_order(payload)

        # Commit the new message as being read
        consumer.commit_offsets()


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yaml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    # logger.info("Connecting to DB. Hostname:" + app_config['datastore']['hostname'] +
    # ", Port:" + app_config['datastore']['port'])
    t1 = Thread(target=process_messages)
    t1.setDaemon(True)
    t1.start()
    app.run(port=8090)

