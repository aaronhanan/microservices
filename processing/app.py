import connexion
import yaml
from apscheduler.schedulers.background import BackgroundScheduler
import json
import logging.config
import os
import datetime
import requests

with open('app_conf.yml', 'r') as f:
    app_config = yaml.safe_load(f.read())

with open('log_conf.yml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

logger = logging.getLogger('basicLogger')


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats, 'interval', seconds=app_config['scheduler']['period_sec'])
    sched.start()


def populate_stats():
    """ Periodically update stats """
    logger.info("Start Processing")
    stats = {}

    if os.path.isfile(app_config['datastore']['filename']):

        stats_file = open(app_config["datastore"]["filename"])

        data = stats_file.read()
        stats = json.loads(data)

        stats_file.close()

    last_updated = datetime.datetime.now()
    if "last_updated" in stats:
        last_updated = stats["last_updated"]

    response = requests.get(app_config["eventstore"]["url"] + "/orders/food-delivery?timestamp=" + str(last_updated))
    if response.status_code == 200:
        if "num_food_orders" in stats.keys():
            stats["num_food_orders"] += len(response.json())
        else:
            stats["num_food_orders"] = len(response.json())

        logger.info("Processed %d Food Orders" % len(response.json()))

    response = requests.get(app_config["eventstore"]["url"] + "/orders/scheduled-delivery?timestamp=" + str(last_updated))
    if response.status_code == 200:
        if "num_scheduled_orders" in stats.keys():
            stats["num_scheduled_orders"] += len(response.json())
        else:
            stats["num_scheduled_orders"] = len(response.json())

        logger.info("Processed %d Scheduled Orders" % len(response.json()))

    stats["last_updated"] = str(datetime.datetime.now())
    stats_file = open(app_config["datastore"]["filename"], "w")

    stats_file.write(json.dumps(stats, indent=4))
    stats_file.close()

    logger.info("Done Processing")


def get_stats():

    logger.info("-> Get Stats Start Processing")
    stats = {}
    if os.path.isfile(app_config['datastore']['filename']):
        stats_file = open(app_config["datastore"]["filename"])
        data = stats_file.read()
        stats_file.close()
        full_stats = json.loads(data)

        if "last_updated" in full_stats:
            stats["last_updated"] = full_stats["last_updated"]
        if "num_food_orders" in full_stats:
            stats["num_food_orders"] = full_stats["num_food_orders"]
        if "num_scheduled_orders" in full_stats:
            stats["num_scheduled_orders"] = full_stats["num_scheduled_orders"]

        logger.info("Found valid stats:")
        logger.debug(stats)
    else:
        return "Stats Do Not Exist", 404

    logger.info("<- Get Stats Done Processing")
    return stats, 200


app = connexion.FlaskApp(__name__, specification_dir='')
app.add_api("openapi.yml", strict_validation=True, validate_responses=True)


if __name__ == "__main__":
    init_scheduler()
    app.run(port=8100)
