# Scraping-orchestra
A scraping Master-slave system based on Google App Engine

This repository showcases an approach to orchestrate from a local process a Scraper deployed in Google App Engine. 
The proposal is a workaround to the **HTTP 429 Too Many Requests Error**. 
The main idea is to redeploy the Scraper to get a new IP whenever the Error shows up.

### Medium article
Take a look at the [article](https://juanluisrto.medium.com/scraping-google-search-without-getting-caught-e43bb91b363e?sk=944b7dc0368b04345a9ad2a2416b311d) I published about this

### System architecture
![alt text](/png/scraper-architecture.png).

### Running locally
To test this locally clone the repo  and run:
* `pip install -r requirements.txt`
* `python master.py` in one terminal
* `gunicorn -b :8080 slave:app  --timeout 360000 --preload` in a different terminal



The output of the master looks like this.

![alt text](/png/terminal.png).

