from flask import Flask, request, jsonify
from google.cloud import storage as gc_storage, logging as gc_logging
import logging, os, time
from threading import Thread
from orchestra.utils import GCloudConnection, Scraper

class Slave(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self, URL)
        self.connect_cloud_services("slave-scraper")
        self.state = "idle"
        self.queue = []
        self.scraper = Scraper()

    def run(self):
        while True:
            if len(self.queue) > 0 and self.state == "idle":
                job = self.queue.pop(0)
                df = self.scrap(job)
                if df == "Failed":
                    self.queue.insert(0,job) # If the job fails, put it back in the queue
                else:
                    self.store(df, str(job))
            else:
                time.sleep(3)

    def store(self, df, filename):
        url = f"gs://{self.URL}/{filename}"
        df.to_csv(url)
        logging.info(f"{filename} stored succesfully")


    def scrap(self, job):
        self.state = "busy"
        try:
            df = self.scraper.scrap(job)
            self.state = "idle"
        except Exception as ex:
            self.state = "scraping-detected"
            logging.error(f"Job {job} failed with an error: {ex}")
            df = "Failed"
        return df


app = Flask(__name__)

@app.route('/job')
def process_job():
    slave.queue.append(request.args)
    return f"Job {request.args} added to the queue"

@app.route('/state')
def current_state():
    return slave.state

@app.route('/queue')
def return_queue():
    return jsonify(slave.queue)


if __name__ == "__main__":
    global slave
    url = "127.0.0.1:8080"
    slave = Slave(url)
    thread = Thread(target=slave.run) # slave runs in parallel with the Flask server
    thread.start()
    app.run(host='127.0.0.1', port=8080, debug=True)