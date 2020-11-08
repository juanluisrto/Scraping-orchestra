import logging, pexpect
import requests
from urllib.parse import urlencode
import os, time, pandas as pd
from utils import GCloudConnection


class Master(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self,URL, LOG_NAME= "master-scraper")
        self.pending_jobs = []
        self.current_job = None

    def restart_machine(self):
        # execute these commands locally to manually re deploy instance
        logging.info("Re-deploying instance")
        deploy = pexpect.spawn('gcloud app deploy --version v1')
        deploy.expect('Do you want to continue (Y/n)?')
        deploy.sendline('Y')

    def start(self):
        try:
            requests.get(f"{self.URL}/start", timeout=3)
        except Exception:
            logging.error("Slave not running")

    def check_slave_state(self):
        try:
            response = requests.get(f"{self.URL}/state", timeout=10)
            state = response.content.decode("utf-8")
        except Exception:
            state = "no-answer"
        return state

    def send_job(self, job):
        url = self.URL + "/job?" + urlencode(job)
        requests.get(url, timeout=10)
        logging.info(f"Sending job = {job} to {url}")

    def orchestrate(self):
        while(len(self.pending_jobs) > 0):
            state = self.check_slave_state()
            logging.info(f"Current state of slave: {state}")
            next_job_ready = False # wont change if state == "busy" or "no-answer"
            if state == "not-started":
                self.start()
            if state == "scraping-detected":  # Error 429 in slave.
                self.pending_jobs.insert(0, self.current_job)
                self.restart_machine()
            elif state == "idle":
                next_job_ready = True
            if next_job_ready:
                self.current_job = self.pending_jobs.pop(0)
                self.send_job(self.current_job)
            time.sleep(3)

    def import_jobs(self):
        df_jobs = pd.read_csv("./csv/sample_jobs.csv", index_col = 0)
        self.pending_jobs = list(df_jobs.to_dict("index").values())


if __name__ == "__main__":
    url = os.getenv("URL")
    if url is None:
        url = "http://0.0.0.0:8080" #local mode
    master = Master(url)
    master.import_jobs()
    master.orchestrate()
