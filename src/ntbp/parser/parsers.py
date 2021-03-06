import os
import json
import pickle
import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
from abc import ABC, abstractmethod

from ntbp.parser import HOST, PREVIEW_URL, BASE_DIR

class _Base(type):
    # Перехватываем момент создания класса
    def __init__(cls, name, bases, attr_dict):
        super().__init__(name, bases, attr_dict)

    # Перехватываем момент создания объекта
    def __call__(cls, *args, **kwargs):
        obj = super().__call__(*args, **kwargs)
        if cls.__name__ == "Preview":
            cls.__bases__[0].page = obj._Preview__num_page
        return obj

class BaseMeta(metaclass=_Base):
    """Base metaclass"""

class BaseParser(BaseMeta):
    # __metaclass__ = ABC
    
    def _get_page(self, url):
        # if hasattr(self, "page"):
            # Подумать над отлавливанием ошибки
            # if self.page < 1:
            #     raise ValueError("Page is < 1")
        response = requests.get(url)
        # if hasattr(self, "page"):
            # Подумать над отлавливанием ошибки
            # if not response.url.endswith(str(self.page)) and self.page != 1:
            #     raise ValueError("Page is very big!")
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
        self.__num_page = kwargs.get("page") if kwargs.get("page") is not None else temp_date
        self.__links = []

    def get_links(self):
        try:
            html = self._get_page(PREVIEW_URL.format(HOST, self.__num_page))
        except ValueError as error:
            print(error)
            # self.__links = []
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

    def __iter__(self):
        self.__cursor = 0
        return self

    def __next__(self):
        if self.__cursor == len(self.__links):
            raise StopIteration
        try:
            return self.__links[self.__cursor]
        finally:
            self.__cursor += 1

    def __getitem__(self, index):
        try:
            print("\n")
            return self.__links[index]
        except TypeError:
            print("Ошибка TypeError")
        except IndexError:
            print("Выход за границы списка")

    def save_to_file(self, name):
        path = os.path.join(BASE_DIR, name + ".bin")
        pickle.dump(self.__links, open(path, "wb"))

    def save_to_json(self, name):
        path = os.path.join(BASE_DIR, name + ".json")
        json.dump(self.__links, open(path, "w"))


class NewsParser(BaseParser):
    def __init__(self, url):
        self._url = url
        self.news = {}

    def get_news(self):
        try:
            html = self._get_page(self._url)
        except ValueError as error:
            print(error)
        else:
            box = html.find("div", attrs={"class": "b-article"})
            if box is not None:
                self.news["head"] = box.find("h1").text
                box_date = box.find("time", attrs={"itemprop": "datePublished"})
                self.news["date"] = datetime.fromisoformat(box_date.get("datetime"))


if __name__ == "__main__":
    parser = Preview(page="03.10.2000")
    parser.get_links()
    print(parser._Preview__links.__len__())
    parser.save_to_json("03.10.2000")
    parser.save_to_file("03.10.2000")    

    news = NewsParser("https://news.tut.by/society/38.html")
    news.get_news()