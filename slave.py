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
        self.state = "idle"
        self.parent, self.child = Pipe()
        self.scraper = Scraper()

    def store(self, df, filename):
        url = f"gs://{self.URL}/{filename}"
        df.to_csv("test.csv")
        logging.info(f"{filename} stored succesfully")

    def scrap(self, job):
        self.child.send("busy")
        self.state = "busy"
        try:
            df = self.scraper.scrap(job)
            self.child.send("idle")
            self.state = "idle"
        except Exception as ex:
            self.set_state("scraping-detected")
            logging.error(f"Job {job} failed with an error: {ex}")
            df = "Failed"
        return df


    def run(self, pipe):
        self.child = pipe
        while True:
            job = self.child.recv()
            if job != None:
                logging.info(f"RUN: {self.state} -- {job}")
                df = self.scrap(job)
                if str(df) != "Failed":
                    self.store(df, str(job))
            else:
                logging.info(f"No Jo. State: {self.state}")
                time.sleep(3)

    def change_state(self, child):
        count = 0
        while True:
            count += 1
            states = ["idle", "busy", "scraping-detected"]
            child.send(states[count%3])
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
    new_state = slave.parent.recv() #gets last update sent by child process
    if new_state is not None:
        slave.state = new_state
    logging.info(f"Current state: {slave.state}")
    return slave.state


if __name__ == "__main__":
    def run(slave):
        slave.run()
    p = Process(target= slave.change_state, args = [slave.child])
    p.start()
    logging.info("Slave run")
    app.run(host='127.0.0.1', port=8080, debug=True)