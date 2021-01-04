import os
import json
import pickle
import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
from abc import ABC, abstractmethod

from fxp.parser import HOST, PREVIEW_URL, BASE_DIR

class BaseParser(ABC):
    def _get_page(self, url):
        response = requests.get(url)
        if response.status_code == 200:
            return BS(response.text, features="html.parser")
        raise ValueError("Response not 200")

    @abstractmethod
    def save_to_file(self, name: str) -> None:
        """Save news to file

        Args:
            name (str): file name
        """

    @abstractmethod
    def save_to_json(self, path: str) -> None:
        """Save news to json file

        Args:
            path (str): file_name
        """

class Preview(BaseParser):
    def __init__(self, **kwargs):
        now = datetime.now()
        temp_date = now.strftime("%d.%m.%Y")
        self.__num_page = kwargs.get("page") or temp_date
        self.__links = []

    def get_links(self):
        try:
            html = self._get_page(PREVIEW_URL.format(HOST, self.__num_page))
        except ValueError:
            self.__links = []
        else:
            top_box = html.find_all("div", attrs={"class": "news-top"})
            box = html.find_all("div", attrs={"class": "b-news"})
            for i in box:
                top_box.append(i)
            if top_box is not None:
                for rubric in top_box:
                    box2 = rubric.find_all("div", attrs={"class": "news-entry"})
                    if box2 is not None:
                        for a in box2:
                            link = a.find("a", attrs={"class": "entry__link"})
                            print(link.get("href"), end='\n\n')
                            self.__links.append(link.get("href"))
            else:
                self.__list = []

    def save_to_file(self, name):
        path = os.path.join(BASE_DIR, name + ".bin")
        pickle.dump(self.__links, open(path, "wb"))

    def save_to_json(self, name):
        path = os.path.join(BASE_DIR, name + ".json")
        json.dump(self.__links, open(path, "w"))


if __name__ == "__main__":
    parser = Preview(page="03.01.2021")
    parser.get_links()
    parser.save_to_json("03.01.2021")
    parser.save_to_file("03.01.2021")