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
        bucket = os.getenv("BUCKET") #define url to bucket where results are stored
        url = f"gs://{bucket}/csv/{filename}" if "CLOUD" in os.environ else f"./csv/{filename}.csv"
        df.to_csv(url)
        logging.info(f"{filename} stored succesfully")

    def scrap(self, job):
        self.child.send("busy") #updates state to stop receiving more jobs
        try:
            df = self.scraper.scrap(job)
            self.child.send("idle")
        except Exception as ex:
            self.child.send("scraping-detected")
            logging.error(f"Job {job} failed with an error: {ex}")
            df = "Failed"
        return df  # returns the job output, or "Failed" if an error arised


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
                time.sleep(3)


app = Flask(__name__)

@app.route('/start')
def start_child_process(): #Gunicorn does not allow the creation of new processes before the app creation, so we need to define this route
    url = os.getenv("BUCKET")
    global slave
    slave = Slave(url)
    p = Process(target=slave.run, args=[slave.child])
    p.start()
    return "Scraper running"

@app.route('/job')
def process_job():
    slave.parent.send(request.args) #sends a job to the Scraper through the "parent" end of the pipe
    return f"Job {request.args} started"

@app.route('/state')
def current_state():
    if slave.parent.poll(timeout=3): #checks if there are new messages from the child process
        slave.state = slave.parent.recv() # updates the state in such case
    return slave.state

if __name__ == "__main__":
    app.run(host='127.0.0.1', port=8080, debug=True)
