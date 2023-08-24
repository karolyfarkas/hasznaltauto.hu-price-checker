import requests
import bs4
import re
import paho.mqtt.client as mqtt
import random
import time
import os
import logging
import sys
import schedule

# Set up env variables
try:
    scrape_url = os.environ["SCRAPE_URL"]
    broker = os.environ["BROKER_HOSTNAME"]
    port = int(os.environ["BROKER_PORT"])
    average_topic = os.environ["AVERAGE_TOPIC"]
    max_topic = os.environ["MAX_TOPIC"]
    min_topic = os.environ["MIN_TOPIC"]
    mqtt_username = os.environ["MQTT_USERNAME"]
    mqtt_password = os.environ["MQTT_PASSWORD"]
except:
    logging.error("Environment variables are not set up correctly!")
    sys.exit()

def job():
    # Scrape site
    try:
        integer_prices = scrape_site(scrape_url)
        average_price = sum(integer_prices) / len(integer_prices)
        max_price = max(integer_prices)
        min_price = min(integer_prices)
        logging.info("Scraping the site succeded.")
        logging.info(f"Calculated prices: Min: { min_price }, Max: { max_price }, Avg: { average_price }")
    except:
        logging.error("Scraping the site failed.")
        return

    # Publish calculated prices to mqtt
    publish_prices(average_price, max_price, min_price)

# Setup variables, logging and schedule
client_id = f'python-mqtt-{random.randint(0, 1000)}'
logging.basicConfig(encoding='utf-8', level=logging.INFO)
schedule.every().day.at("09:00", "Europe/Budapest").do(job)
schedule.every().day.at("12:00", "Europe/Budapest").do(job)

def scrape_site(scrape_url):
    result = requests.get(scrape_url)
    soup = bs4.BeautifulSoup(result.text, "lxml")
    integer_prices = []

    for desktop_div in soup.select(".price-fields-desktop"):
        for price_div in desktop_div.select(".pricefield-primary"):
            price_wo_spaces = re.sub('\s+', '', price_div.getText())
            integer_price = int(price_wo_spaces.removesuffix("Ft"))
            integer_prices.append(integer_price)
    return integer_prices

def publish_prices(average_price, max_price, min_price):
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            logging.info("Connected to MQTT Broker!")
        else:
            logging.error(f"Failed to connect to MQTT broker, return code { rc }")

    client = mqtt.Client(client_id)
    client.username_pw_set(mqtt_username, mqtt_password)
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    time.sleep(2) # on_connect callback is async so we have to wait for the connection
    
    def publish_price(topic, price):  
        status, _ = client.publish(topic, price, qos=1)
        if status == 0:
            logging.info(f"Message sent to {topic}")
        else:
            logging.error(f"Failed to send message to topic {topic}")
    
    publish_price(average_topic, average_price)
    publish_price(max_topic, max_price)
    publish_price(min_topic, min_price)

    client.loop_stop() 

# Run the job on start
job()

# Run the job as per the schedule
while(True):
    schedule.run_pending()
    time.sleep(1)
