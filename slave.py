from flask import Flask, request, jsonify
from google.cloud import storage as gc_storage, logging as gc_logging
import logging, os, time
from multiprocessing import Process, Pipe
from utils import GCloudConnection, Scraper

class Slave(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self,URL, LOG_NAME= "slave-scraper")
        self.state = "idle"
        self.parent, self.child = Pipe()
        self.scraper = Scraper()

    def store(self, df, filename):
        BUCKET = "stock-sentiment-nlp.appspot.com"
        url = f"gs://{BUCKET}/csv/{filename}"
        df.to_csv(url)
        logging.info(f"{filename} stored succesfully")

    def scrap(self, job):
        self.child.send("busy")
        try:
            df = self.scraper.scrap(job)
            self.child.send("idle")
        except Exception as ex:
            self.child.send("scraping-detected")
            logging.error(f"Job {job} failed with an error: {ex}")
            df = "Failed"
        return df


    def run(self, pipe):
        self.child = pipe
        while True:
            job = self.child.recv()
            if job != None:
                logging.info(f"Running job: {job}")
                df = self.scrap(job)
                if str(df) != "Failed":
                    self.store(df, "_".join(job.values()))
            else:
                logging.info("Waiting for jobs")
                time.sleep(3)


app = Flask(__name__)

@app.route('/start')
def start_child_process():
    url = os.environ["URL"]
    global slave
    slave = Slave(url)
    p = Process(target=slave.run, args=[slave.child])
    p.start()
    logging.info("Slave is running")
    return "Scraper running"

@app.route('/job')
def process_job():
    logging.info(request.args)
    slave.parent.send(request.args)
    return f"Job {request.args} started"

@app.route('/state')
def current_state():
    if slave.parent.poll(timeout=3): #checks if there are new messages from the child process
        slave.state = slave.parent.recv() # updates the state in such case
    logging.info(f"Current state: {slave.state}")
    return slave.state




if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080, debug=True)
