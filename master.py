import logging, pexpect
import requests
from urllib.parse import urlencode
import os, time
from utils import GCloudConnection


class Master(GCloudConnection):

    def __init__(self, URL):
        GCloudConnection.__init__(self,URL, LOG_NAME= "master-scraper")
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
                self.restart_machine()
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
        self.pending_jobs.extend([{"query" : "Apple", "start": "2019-01-01", "end" : "2019-01-06"},
                                  {"query" : "Tesla", "start": "2019-03-01", "end" : "2019-03-03"},
                                  {"query" : "Space-X", "start": "2019-02-01", "end" : "2019-06-03"},
                                  {"query" : "Warren Buffet", "start": "2019-03-01", "end" : "2019-03-03"}])


if __name__ == "__main__":
    url = os.environ["URL"]
    url = "http://scrap-orchestra-dot-stock-sentiment-nlp.wl.r.appspot.com"
    master = Master(url)
    master.import_jobs()
    if master.start():
        master.orchestrate()
