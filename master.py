import logging, pexpect
import requests
from urllib.parse import urlencode
import pandas as pd, time
from utils import GCloudConnection


class Master(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self,URL)
        #self.connect_cloud_services("master-scraper")
        self.pending_jobs = []
        self.current_job = None

    def restart_machine(self):
        logging.info("Restarting instance")
        shutdown = pexpect.spawn('gcloud app versions stop v1') #execute these commands locally to manually relaunch instance
        shutdown.expect('Do you want to continue (Y/n)?')
        shutdown.sendline('Y')

        deploy = pexpect.spawn('gcloud app deploy --version v1') #execute these commands locally to manually relaunch instance
        deploy.expect('Do you want to continue (Y/n)?')
        deploy.sendline('Y')

    def start(self):
        try:
            requests.get(f"{self.URL}/start", timeout=3)
            return True
        except Exception:
            logging.error("Slave not running")
            return False

    def check_slave_state(self):
        try:
            response = requests.get(f"{self.URL}/state", timeout=10)
            if response.status_code == 200: #if the endpoint answers
                state = response.content.decode("utf-8")
        except Exception:
            state = "no-answer"
        return state

    def get_pending_jobs(self):
        response = requests.get(f"{self.URL}/queue", timeout=10)
        return response.json()

    def send_job(self, job):
        url = self.URL + "/job?" + urlencode(job)
        requests.get(url, timeout=10)
        logging.info(f"Sending job = {job} to {url}")

    def orchestrate(self):
        while(len(self.pending_jobs) > 0):
            state = self.check_slave_state()
            logging.info("Current state: " + state)
            next_job_ready = False
            if state == "scraping-detected":  # Error 429 in slave.
                self.pending_jobs.insert(0, self.current_job)
                #self.restart_machine()
            elif state == "busy" or state == "no-answer":  # Job being currently run or server relaunching
                next_job_ready = False
            elif state == "idle":
                next_job_ready = True
            if next_job_ready:
                self.current_job = self.pending_jobs.pop(0)
                self.send_job(self.current_job)
            time.sleep(5)

    def import_jobs(self):
        #df_jobs = pd.read_csv("/csv/sample_jobs.csv")
        #self.pending_jobs = list(df_jobs.to_dict("index").values())
        self.pending_jobs.extend([{"query" : "Apple", "start": "2019-01-01", "end" : "2019-01-02"},
                                 {"query" : "Tesla", "start": "2019-03-01", "end" : "2019-03-03"}])

    def test_state(self):
        while True:
            logging.info("Current state: " + self.check_slave_state())
            time.sleep(3)



if __name__ == "__main__":
    url = "http://127.0.0.1:8080"
    master = Master(url)
    master.import_jobs()
    if master.start():
        #master.test_state()
        master.orchestrate()
