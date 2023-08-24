FROM python:slim-bullseye

COPY scrape.py .

RUN pip install bs4 paho-mqtt schedule requests pytz lxml

CMD ["python", "scrape.py"]