import logging, os
import requests
from urllib.parse import urlencode
import pandas as pd, time
from orchestra.utils import GCloudConnection


class Master(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self,URL)
        self.logger = self.connect_cloud_services("master-scraper")
        self.pending_jobs = []

    def restart_machine(self):
        logging.info("Restarting instance")
        os.system("gcloud app versions stop v1") #execute these commands locally to manually relaunch instance
        time.sleep(20)
        os.system("gcloud app versions start v1") # a new IP will be used and scraping can be resumed

    def check_slave_state(self):
        response = requests.get(f"{self.URL}/state", timeout=10)
        if response.status_code == 200: #if the endpoint answers
            state = response.content.decode("utf-8")
            next_job_ready = False
            if state == "scraping-detected": #Error 429 in slave.
                slave_queue = self.get_pending_jobs()
                self.pending_jobs = slave_queue + self.pending_jobs
                self.restart_machine()
            elif state == "busy": #Job being currently run
                next_job_ready = False
            elif state == "idle":
                next_job_ready = True
        else:
            next_job_ready = False # Currently rebooting
        return next_job_ready

    def get_pending_jobs(self):
        response = requests.get(f"{self.URL}/state", timeout=10)
        return response.json()

    def send_job(self, job):
        url = self.URL + "&" + urlencode(job)
        requests.get(f"{url}/job", timeout=10)

    def orchestrate(self):
        while(len(self.pending_jobs) > 0):
            if self.check_slave_state():
                self.send_job(self.pending_jobs.pop(0))
            else:
                time.sleep(3)

    def import_jobs(self):
        df_jobs = pd.read_csv("/csv/sample_jobs.csv")
        self.pending_jobs = list(df_jobs.to_dict("index").values())



if __name__ == "__main__":
    url = "127.0.0.1:8080"
    master = Master(url)
    master.import_jobs()
    master.orchestrate()
