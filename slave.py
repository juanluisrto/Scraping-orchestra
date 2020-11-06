from flask import Flask, request, jsonify
from google.cloud import storage as gc_storage, logging as gc_logging
import logging, os, time
from multiprocessing import Process, Pipe, Value
from utils import GCloudConnection, Scraper
import ctypes

class Slave(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self, URL)
        #self.connect_cloud_services("slave-scraper")
        self.state = Value("i", 0)
        self.sender, self.receiver = Pipe()
        self.scraper = Scraper()

    def store(self, df, filename):
        url = f"gs://{self.URL}/{filename}"
        df.to_csv("test.csv")
        logging.info(f"{filename} stored succesfully")

    def get_state(self):
        states = { 0: "idle", 1: "busy", 2: "scraping-detected"}
        return states[self.state.value]

    def set_state(self, state):
        states = {"idle" : 0, "busy" : 1, "scraping-detected": 2}
        self.state = states[state]

    def scrap(self, job):
        self.set_state("busy")
        try:
            df = self.scraper.scrap(job)
            self.set_state("idle")
        except Exception as ex:
            self.set_state("scraping-detected")
            logging.error(f"Job {job} failed with an error: {ex}")
            df = "Failed"
        return df


    def run(self, state, receiver):
        self.state = state
        while True:
            job = receiver.recv()
            if job != None:
                logging.info(f"RUN: {self.get_state()} -- {job}")
                df = self.scrap(job)
                if str(df) != "Failed":
                    self.store(df, str(job))
            else:
                logging.info(f"No Jo. State: {self.get_state()}")
                time.sleep(3)

    def change_state(self, state, receiver):
        while True:
            state.value = state.value % 3
            time.sleep(4)

app = Flask(__name__)
url = "127.0.0.1:8080"
slave = Slave(url)

@app.route('/job')
def process_job():
    logging.info(request.args)
    slave.sender.send(request.args)
    return f"Job {request.args} started"

@app.route('/state')
def current_state():
    logging.info(f"Current state: {slave.get_state()}")
    return slave.get_state()


if __name__ == "__main__":
    def run(slave):
        slave.run()
    p = Process(target= slave.change_state, args = (slave.state, slave.receiver))
    p.start()
    logging.info("Slave run")
    app.run(host='127.0.0.1', port=8080, debug=True)