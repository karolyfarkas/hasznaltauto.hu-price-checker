import requests
import bs4
import re
import paho.mqtt.client as mqtt
import random
import time
import os
import logging

scrape_url = os.environ["SCRAPE_URL"]
broker = os.environ["BROKER_HOSTNAME"]
port = int(os.environ["BROKER_PORT"])
average_topic = os.environ["AVERAGE_TOPIC"]
max_topic = os.environ["MAX_TOPIC"]
min_topic = os.environ["MIN_TOPIC"]
username = os.environ["USERNAME"]
password = os.environ["PASSWORD"]

client_id = f'python-mqtt-{random.randint(0, 1000)}'

logging.basicConfig(encoding='utf-8', level=logging.INFO)

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
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    client.loop_start()
    time.sleep(2) # on_connect callback is async so we have to wait for the connection
    client.publish(average_topic, average_price)
    client.publish(max_topic, max_price)
    client.publish(min_topic, min_price)
    client.loop_stop() 

integer_prices = scrape_site(scrape_url)
average_price = sum(integer_prices) / len(integer_prices)
max_price = max(integer_prices)
min_price = min(integer_prices)

print("Maximum price: ", max_price)
print("Minimum price: ", min_price)
print("Average price: ", average_price)

publish_prices(average_price, max_price, min_price)