import time
import json
import os
import requests
import warnings
import pandas as pd
import numpy as np
import re
import threading
import logging
import csv
import sys
from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from datetime import date
from urllib.request import Request, urlopen
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from random import randint
from configparser import ConfigParser
from pathlib import Path

from chrome import ChromeDriverManager

from s3_util import upload_job_status
from s3_util import upload_local_files_to_s3
from s3_util import get_s3_bucket


class EXRScraper:

    def __init__(self):

        self.chrome_driver_path = (os.path.join(os.path.dirname(__file__), 'chromedriver.exe')) #ChromeDriverManager().install()
        # Config File
        self.cfg = ConfigParser()
        self.cfg.read(os.path.join(os.path.dirname(__file__), 'exr_config.ini'))

        # Creating folder for later use
        if not os.path.exists(self.cfg.get('log', 'path')):
            os.makedirs(self.cfg.get('log', 'path'))

        if not os.path.exists(self.cfg.get('data', 'data_folder')):
            os.makedirs(self.cfg.get('data', 'data_folder'))

        # Proxies
        # self.PROXIES = {
        #    'http': 'http.proxy.aws.fmrcloud.com:8000',
        #  'https': 'http.proxy.aws.fmrcloud.com:8000'
        #   }

        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36'
        }

        warnings.filterwarnings("ignore")
        self.today = date.today()
        self.states = ["Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota"]
        #self.states = ["Alabama", "Arizona", "California", "Colorado", "Connecticut", "Delaware", "District_of_Columbia","Florida", "Georgia", "Idaho", "Illinois", "Indiana", "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri", "Nebraska", "Nevada", "New_Hampshire", "New_Jersey", "New_Mexico", "New_York", "North_Carolina", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", "Rhode_Island", "South_Carolina", "Tennessee", "Texas", "Utah", "Virginia", "Washington", "Wisconsin"]
        #self.states = ["Delaware", "District_of_Columbia", "Wisconsin", "Alabama", "Connecticut","Idaho"]
        #self.states = ["Rhode_Island"]
        self.location_df = []
        self.exr_df = pd.DataFrame(
            columns=["EFFECTIVE_DT", "SITE_NAME", "COMPANY_NAME", "LOCATION_ID", "LOC_ADDRESS", "LOC_STATE",
                     "LOC_LATITUDE", "LOC_LONGITUDE", "STORAGE_SIZE", "SIZE_ALT", "STORAGE_AMENITIES", "CONCESSIONS",
                     "FULL_PRICE", "SALE_PRICE", "LOCATION_URL", "EFFECTIVE_PRICE"])

    # Configure logging to print info while program runs
    def configure_logging(self):
        self.logger = logging.getLogger('EXR')
        self.logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        log_file_name = 'exr_log_{}.log'.format(self.today.strftime("%Y_%m_%d"))
        log_file = os.path.join(self.cfg.get('log', 'path'), log_file_name)
        logHandler = logging.FileHandler(log_file)
        logHandler.setLevel(logging.DEBUG)
        logHandler.setFormatter(formatter)

        self.logger.addHandler(logHandler)

        consoleHandler = logging.StreamHandler(sys.stdout)
        consoleHandler.setLevel(logging.DEBUG)
        consoleHandler.setFormatter(formatter)
        self.logger.addHandler(consoleHandler)

    def scrapeState(self, ii):
        while True:
            self.logger.info(ii)
            SLEEP_TIME = randint(2, 7)
            url = "https://www.extraspace.com/storage/facilities/us/" + ii + "/"
            self.logger.info(url)
            response = requests.get(url=url,
                                    headers=self.headers,
                                    verify=False)
            soup = BeautifulSoup(response.content)
            self.logger.info(
                "Cities: " + str(len(soup.find_all("div", attrs={"class": "city-section"}))))
            if len(soup.find_all("div", attrs={"class": "city-section"})) == 0:
                continue
            cities = soup.find_all("div", attrs={"class": "city-section"})
            for jj in cities:
                for kk in jj.find_all("a", attrs={'class': 'store-details'}):
                    temp_dict = {}
                    temp_dict["state"] = ii
                    temp_dict["address"] = kk.find('span', attrs={'class': 'address'}).text.strip()
                    temp_dict["location_url"] = kk["href"]
                    temp_dict["location_id"] = temp_dict["location_url"].split("/")[-2]

                    self.location_df.append(temp_dict)
                    self.logger.info(temp_dict)
            time.sleep(SLEEP_TIME)
            break

    def scrapeLocation(self, linkDict):

        warnings.filterwarnings("ignore")
        exr_excp_df = pd.DataFrame(columns=["category", "url"])
        ii = linkDict["location_url"]
        self.logger.info("Scraping : " + ii)
        retryAttempts = 0
        while retryAttempts < 3:

            retryNeeded = False
            SLEEP_TIME = randint(2, 7)

            # prefs = {"profile.managed_default_content_settings.images": 2}
            # options = webdriver.ChromeOptions()
            options = Options()
            # options.add_experimental_option("prefs", prefs)
            options.add_argument("--no-sandbox")
            # options.add_argument("--start-maximized")
            # options.add_argument("--force-device-scale-factor=0.5")
            # options.add_argument("--headless")

            try:
                driver = webdriver.Chrome(self.chrome_driver_path, chrome_options=options)
                driver.get(ii)
                time.sleep(3)
                num_units = len(driver.find_elements_by_xpath("//a[@data-qa='reserve-button']"))

            except Exception as e:
                self.logger.info(e)
                self.logger.info("failed to get page. Retrying")
                retryAttempts = retryAttempts + 1
                retryNeeded = True
                break

            try:
                soup = BeautifulSoup(driver.page_source)
                geoCordsJSON = soup.find_all("script", type="application/ld+json")[1].text
                geoCords = re.findall('latitude.*?([0-9]+.[0-9]*).*?longitude.*?(-[0-9]+.[0-9]*)', geoCordsJSON)
                lati = geoCords[0][0]
                longi = geoCords[0][1]
            except:
                lati = ""
                longi = ""
            print(num_units)
            for jj in range(num_units):
                temp_dict = {}
                temp_dict["size"] = ""
                temp_dict["full_price"] = ""
                temp_dict["sale_price"] = ""
                temp_dict["amenities"] = ""
                temp_dict["location_id"] = ii.split("/")[-2]
                try:
                    # try:
                    # size = driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text
                    # print(f"Size info : {size}")
                    # except Exception as e:
                    #  print("Size issue")
                    temp_dict["size"] = \
                        driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text.split("\n")[
                            2].strip()
                except:
                    temp_dict_excp = {}
                    temp_dict_excp["category"] = "size"
                    temp_dict_excp["url"] = ii
                    exr_excp_df = exr_excp_df.append(temp_dict_excp, ignore_index=True)
                try:
                    temp_dict["full_price"] = \
                        driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text.split("IN STORE")[
                            1].split("\n")[1].strip()
                except:
                    temp_dict_excp = {}
                    temp_dict_excp["category"] = "full_price"
                    temp_dict_excp["url"] = ii
                    exr_excp_df = exr_excp_df.append(temp_dict_excp, ignore_index=True)

                try:
                    temp_dict["sale_price"] = \
                        driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text.split("IN STORE")[
                            1].split("\n")[3].strip()
                except:
                    try:
                        temp_dict["sale_price"] = \
                            driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text.split(
                                "WEB RATE")[
                                1].split("\n")[1].strip()
                    except:
                        temp_dict_excp = {}
                        temp_dict_excp["category"] = "sale_price"
                        temp_dict_excp["url"] = ii
                        exr_excp_df = exr_excp_df.append(temp_dict_excp, ignore_index=True)

                try:
                    amenities = driver.find_elements_by_xpath("//div[@data-qa='features']")[jj].text.replace('\n', ', ').strip()
                    print(f"Amenities :  {amenities}")
                    temp_dict["amenities"] = amenities
                except:
                    temp_dict_excp = {}
                    temp_dict_excp["category"] = "amenities"
                    temp_dict_excp["url"] = ii
                    exr_excp_df = exr_excp_df.append(temp_dict_excp, ignore_index=True)
                    temp_dict["amenities"] = ""

                temp_dict["concessions"] = ""
                try:
                    temp_dict["concessions"] = \
                        driver.find_elements_by_xpath("//div[@data-qa='unit-class-card']")[jj].text.split('SELECT\n')[
                            1].strip()
                except:
                    temp_dict_excp = {}
                    temp_dict_excp["category"] = "concessions"
                    temp_dict_excp["url"] = ii
                    exr_excp_df = exr_excp_df.append(temp_dict_excp, ignore_index=True)

                if temp_dict["full_price"] != "" and temp_dict["sale_price"] == "":
                    temp_dict["sale_price"] = temp_dict["full_price"]
                if temp_dict["sale_price"] != "" and temp_dict["full_price"] == "":
                    temp_dict["full_price"] = temp_dict["sale_price"]
                try:
                    pct_off = temp_dict["concessions"]
                    if 'FIRST MONTH HALF OFF*' in pct_off:
                        effective_price = round(float(temp_dict["sale_price"][1:]) * (23 / 24), 2)
                        temp_dict["effective_price"] = "$" + str(effective_price)
                    elif "FIRST MONTH FREE*" in pct_off:
                        effective_price = round(float(temp_dict["sale_price"][1:]) * (11 / 12), 2)
                        temp_dict["effective_price"] = "$" + str(effective_price)
                    elif "2ND MONTH FREE*" in pct_off:
                        effective_price = round(float(temp_dict["sale_price"][1:]) * (5 / 6))
                        temp_dict["effective_price"] = "$" + str(effective_price)
                    elif "50% OFF 1ST 3 MONTHS*" in pct_off:
                        effective_price = round(float(temp_dict["sale_price"][1:]) * (21 / 24), 2)
                        temp_dict["effective_price"] = "$" + str(effective_price)
                    else:
                        temp_dict["effective_price"] = temp_dict["sale_price"]
                except:
                    temp_dict["effective_price"] = temp_dict["sale_price"]

                entry = {"EFFECTIVE_DT": self.today.strftime("%Y_%m_%d"), "SITE_NAME": "EXR",
                         "COMPANY_NAME": "Extra Space Storage Inc.", "LOCATION_ID": linkDict['location_id'],
                         "LOC_ADDRESS": linkDict['address'], "LOC_STATE": linkDict['state'], "LOC_LATITUDE": lati,
                         "LOC_LONGITUDE": longi, "STORAGE_SIZE": temp_dict["size"],
                         "STORAGE_AMENITIES": temp_dict["amenities"], "CONCESSIONS": temp_dict["concessions"],
                         "FULL_PRICE": temp_dict["full_price"], "SALE_PRICE": temp_dict["sale_price"],
                         "LOCATION_URL": ii, "EFFECTIVE_PRICE": temp_dict["effective_price"]}
                if entry["LOC_LATITUDE"] == "" or entry["STORAGE_SIZE"] == "":
                    driver.quit()
                    time.sleep(SLEEP_TIME)
                    retryAttempts = retryAttempts + 1
                    retryNeeded = True
                    self.logger.info("Retry attempted!")
                    break
                else:
                    self.exr_df = self.exr_df.append(entry, ignore_index=True)
                    self.logger.info(entry)
            if num_units > 0 and (
                    temp_dict["full_price"] == "" or temp_dict["size"] == "" or lati == ""): retryNeeded = True
            if retryNeeded and retryAttempts < 3:
                continue
            elif retryAttempts == 3:
                self.logger.info("FAIL WARNING: " + ii + " failed to collect data.")
            time.sleep(SLEEP_TIME)
            driver.quit()
            break

    def exportToFinalCSV(self):
        self.endTime = date.today()
        if len(self.exr_df.index) < 500:
            self.logger.error('COLLECTED LESS THAN 500 UNITS DURING ENTIRE SCRAPE - EXPECTED >20k')
            sys.exit(1)
        try:
            self.data_folder = os.path.join(self.cfg.get('data', 'data_folder'), self.endTime.strftime("%Y-%m-%d"))
            Path(self.data_folder).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.logger.error('Failed to configure data folder, check exr_config.ini')
            raise e
        data_file_name = 'exr_data_{}-4.csv'.format(self.endTime.strftime("%Y-%m-%d"))
        data_file = os.path.join(self.data_folder, data_file_name)
        self.exr_df.to_csv(data_file, header=False, encoding='utf-8')
        self.logger.info(f"Exported CSV to : {data_file}")

        merged_directory = self.data_folder
        s3_uploadMergedFolder = merged_directory.replace('\\', '/')
        s3_uploadMergedFolder = s3_uploadMergedFolder.replace('C:/ada_apps/data', 'appdata')

        if not os.path.exists(merged_directory):
            os.makedirs(merged_directory)

        # csv_name = f"sketchers_stock_merged1_{current_datetime.strftime('%Y-%m-%d')}.csv"
        merge_file_location = os.path.join(merged_directory, data_file_name)

        s3_bucket = get_s3_bucket()

        print(f"s3_uploadMergedFolder: {s3_uploadMergedFolder}")
        print(f"merged_directory: {merged_directory}")
        print(f"merge_file_location: {merge_file_location}")

        upload_local_files_to_s3(
            s3_bucket,
            self.data_folder,
            s3_uploadMergedFolder,
            data_file_name,
            content_type='text/html'
        )

    def upload_logs_to_s3(self):

        self.data_folder = os.path.join(self.cfg.get('data', 'data_folder'), self.endTime.strftime("%Y-%m-%d"))
        Path(self.data_folder).mkdir(parents=True, exist_ok=True)

        log_file_name = 'exr_log_{}.log'.format(self.today.strftime("%Y_%m_%d"))
        s3_bucket = get_s3_bucket()

        merged_directory = self.data_folder
        s3_uploadMergedFolder = merged_directory.replace('\\', '/')
        s3_uploadMergedFolder = s3_uploadMergedFolder.replace('C:/ada_apps/data', 'appdata')

        upload_local_files_to_s3(
            s3_bucket,
            self.cfg.get('log', 'path'),
            s3_uploadMergedFolder,
            log_file_name,
            content_type='text/html',
        )

    def process(self):
        self.logger.info("EXR job START")

        MAX_THREADS = int(self.cfg.get('thread_count', 'count'))

        for state in self.states:
          self.scrapeState(state)

        LocationPool = ThreadPool(processes=MAX_THREADS)
        while len(self.location_df) != 0:
            locationDict = self.location_df.pop()
            LocationPool.apply_async(self.scrapeLocation, (locationDict,))
            # break

        LocationPool.close()
        LocationPool.join()


if __name__ == "__main__":
    try:
        scraper = EXRScraper()
        scraper.configure_logging()
        scraper.process()
        scraper.exportToFinalCSV()
        scraper.upload_logs_to_s3()
    except Exception as e:
        print(e)

__author__ = "Anil - a485047"
