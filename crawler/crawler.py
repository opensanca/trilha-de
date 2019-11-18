from google.cloud import pubsub

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import argparse
import datetime
import json
import logging
import uuid

class Crawler:
    def __init__(self, topic, max_iterations, browser_options):
        self.topic = topic
        self.max_iterations = max_iterations

        chrome_options = Options()
        
        for option in browser_options:
            chrome_options.add_argument(option)
        
        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.get('https://theuselessweb.com/')

        self.pubsub_client = pubsub.PublisherClient()

    def __del__(self):
        self.driver.quit()

    def __send_to_pubsub(self, link, page_source):
        self.pubsub_client.publish(topic=self.topic, data=bytes(json.dumps({
            "id": uuid.uuid4().hex,
            "content": page_source,
            "created_at": datetime.datetime.now().isoformat()
        }).encode('utf-8')))

    def __get_links(self, max_iterations):
        for _ in range(max_iterations):
            button = self.driver.find_element_by_xpath('//*[@id="button"]')
            button.click()
            self.driver.switch_to.window(self.driver.window_handles[1])
            logging.info(f'Got url ${self.driver.current_url}')
            yield (self.driver.current_url, self.driver.page_source)
            self.driver.close()
            self.driver.switch_to.window(self.driver.window_handles[0])
        
        self.driver.quit()

    def craw(self):
        for link, page_source in self.__get_links(max_iterations=self.max_iterations):
            self.__send_to_pubsub(link, page_source)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", type=str, help='Destination topic', required=True)
    parser.add_argument("--max_iterations", type=int, help='Max number of requests', default=1000)
    known_args, other = parser.parse_known_args()

    crawler = Crawler(topic=known_args.topic, max_iterations=known_args.max_iterations, browser_options=other)
    crawler.craw()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
