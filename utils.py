from google.cloud import logging as gc_logging
import pandas as pd
from googlesearch import search, get_tbs
import os

class GCloudConnection:

    def __init__(self, URL):
        # env variable declared only for gcloud authentication during local tests. Not necessary at deployed instances
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = './stock-sentiment-nlp.json'
        self.URL = URL

    def connect_cloud_services(self, LOG_NAME):
            # connect gcloud logger to default logging.
            logging_client = gc_logging.Client()
            logging_client.get_default_handler()
            logging_client.setup_logging().logger(LOG_NAME)

class Scraper:

    def scrap(self, query, from_date, to_date, number_of_urls = 10):
        tbs = get_tbs(from_date=from_date, to_date=to_date) #"%Y-%m-%d"
        results = search(query, tbs=tbs, pause=5, stop=number_of_urls)
        urls = []
        for url in results:
            urls.append(url)
        return pd.DataFrame(urls, columns=["urls"])