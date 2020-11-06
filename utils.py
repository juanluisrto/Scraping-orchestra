from google.cloud import logging as gc_logging
import pandas as pd
from googlesearch import search, get_tbs
import os, logging

class GCloudConnection:

    def __init__(self, URL, LOG_NAME):
        # env variable declared only for gcloud authentication during local tests. Not necessary at deployed instances
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './credentials.json'
        logging.getLogger().setLevel(logging.INFO)
        self.connect_cloud_services(LOG_NAME)
        self.URL = URL

    def connect_cloud_services(self, LOG_NAME):
            # connect gcloud logger to default logging.
            logging_client = gc_logging.Client()
            logging_client.get_default_handler()
            logging_client.setup_logging()
            logging_client.logger(LOG_NAME)

class Scraper:
    # runs same query filtering by every date in date range
    def scrap(self, job, number_of_urls = 10):
        query, from_date, to_date = job.values()
        urls = []
        for d in pd.date_range(from_date, to_date):
            tbs = get_tbs(from_date=d, to_date=d) #"%Y-%m-%d"
            results = search(query, tbs=tbs, pause=2, stop=number_of_urls)
            for url in results:
                urls.append({"date" : d.date(), "url" : url})
        return pd.DataFrame(urls, columns=["date", "url"])