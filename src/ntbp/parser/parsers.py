import os
import json
import pickle
import requests
import time
import sqlite3
from random import randint
from threading import Lock
from datetime import datetime, timedelta
from bs4 import BeautifulSoup as BS
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


from ntbp.parser import HOST, PREVIEW_URL, BASE_DIR


lock = Lock()
count = 1


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
        if hasattr(self, "page"):
            min_date = datetime.strptime("03.10.2000", "%d.%m.%Y")
            page_date = datetime.strptime(self.page, "%d.%m.%Y")
            if page_date < min_date:
                raise ValueError("Page is < 03.10.2000")
        response = requests.get(url)
        if hasattr(self, "page"):
            max_date = datetime.now()
            page_date = datetime.strptime(self.page, "%d.%m.%Y")
            if page_date > max_date:
                raise ValueError("Page is > temp date")
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
        global count  #
        count = 1   #
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
                            # print(link.get("href"), end='\n\n')
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
            # if type(index) == int:
            if isinstance(index, int):
                res = self.__links[index]
                return res
            # elif type(index) == slice:  
            elif isinstance(index, slice):    
                obj = Preview()
                obj._Preview__links = self.__links[index]
                return obj
            else:
                raise TypeError
        except TypeError:
            print("Ошибка TypeError. Ожидается int или slice")
        except IndexError:
            print("Выход за границы списка")

    def save_to_file(self, name):
        path = os.path.join(BASE_DIR, name + ".bin")
        pickle.dump(self.__links, open(path, "wb"))

    def save_to_json(self, name):
        path = os.path.join(BASE_DIR, name + ".json")
        json.dump(self.__links, open(path, "w"))


class NewsParser(BaseParser):
    def __init__(self):
        self.news = {}

    def __call__(self, url):
        try:
            html = self._get_page(url)
        except ValueError as error:
            print(error)
        else:
            with lock:
                box = html.find("div", attrs={"class": "b-article"})
                if box is not None:
                    self.news["head"] = box.find("h1").text
                    box_date = box.find("time", attrs={"itemprop": "datePublished"})
                    if box_date is not None:
                        self.news["date"] = datetime.fromisoformat(box_date.get("datetime")).timestamp()
                    list_img =  box.find_all("img")
                    self.news["img_link"] = []
                    if list_img is not None:
                        for img in list_img:
                            self.news["img_link"].append(img.attrs["src"])
                    if box.find("div", attrs={"id": "article_body"}) is not None:
                        text_block = box.find("div", attrs={"id": "article_body"}).text
                        self.news["text"] = text_block 
                    list_source_link = box.find_all("a", attrs={"itemprop": "sameAs"})
                    if list_source_link is not None:
                        link_list = []
                        for el in list_source_link:
                            link_list.append(el.attrs["href"])
                        self.news["source_link"] = link_list
                    else:
                        self.news["source_link"] = ["news.tut.by"]
                    a_rubric = html.find("div", attrs={"class": "b-label"})
                    self.news["rubric"] = a_rubric.find("a").text
                    self.news["tags"] = []
                    box_tag = html.find("ul", attrs={"class": "b-article-info-tags"})
                    if box_tag is not None:
                        list_tag = box_tag.find_all("a")
                        for a in list_tag:
                            self.news["tags"].append(a.text)
                    # print(count, self.news["tags"])
                    # self.save_to_json()
                    self.save_to_db()
                    print(self.news["head"] + "\tOK...")

    def save_to_json(self):
        global count
        # print(count, len(self.news["head"]), self.news["head"])
        
        date_str = datetime.fromtimestamp(self.news["date"]).strftime("%Y-%m-%d")    # 2021-01-19
        path_list = []
        path_list.append(date_str[:4])
        path_list.append(date_str[5:7])
        path_list.append(date_str[8:10])
        # path_list.append(head)
        path_list.append(str(count))
        in_path = os.path.join(*path_list)
        path = os.path.join(BASE_DIR, in_path + ".json")    #
        dirs = os.path.split(path)[:-1]
        try:
            os.makedirs(os.path.join(*dirs))
        except Exception as error:
            print(error)
        json.dump(self.news, open(path, "w", encoding="utf-8"), ensure_ascii=False)
        count += 1

    def save_to_db(self):
        connection = sqlite3.connect("news_tut_by.db")
        cursor = connection.cursor()
        sql = """
            --1
            CREATE TABLE IF NOT EXISTS rubrics(
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "name" TEXT(30) NOT NULL UNIQUE
            );
            
            --2
            CREATE TABLE IF NOT EXISTS "sources"(
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "name" TEXT(70) NOT NULL UNIQUE
            );
            
            --3
            CREATE TABLE IF NOT EXISTS tags(
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                "name" TEXT(30) NOT NULL UNIQUE
            );
            
            --4
            CREATE TABLE IF NOT EXISTS news(
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                title TEXT(256) NOT NULL,
                "text" TEXT NOT NULL,
                "timestamp" REAL NOT NULL,
                img_link TEXT,
                rubric_id INTEGER NOT NULL,
                UNIQUE (title, timestamp),
                FOREIGN KEY (rubric_id) REFERENCES rubrics(id) ON DELETE CASCADE ON UPDATE CASCADE
            );
            
            --5
            CREATE TABLE IF NOT EXISTS tags_news(
                id_tag INTEGER NOT NULL,
                id_news INTEGER NOT NULL,
                FOREIGN KEY (id_tag) REFERENCES tags(id),
                FOREIGN KEY (id_news) REFERENCES news(id)
            );
            
            --6
            CREATE TABLE IF NOT EXISTS sources_news(
                id_source INTEGER NOT NULL,
                id_news INTEGER NOT NULL,
                FOREIGN KEY (id_source) REFERENCES sources(id),
                FOREIGN KEY (id_news) REFERENCES news(id)
            );"""
        cursor.executescript(sql)
        # list_rubrics = [("Sport",), ("Politic",), ("Finance",), ("IT",), ("Sport",), ("IT",), ("Sport",)]
        cursor.execute("INSERT OR IGNORE INTO rubrics (name) VALUES(?)", (self.news["rubric"],))
        list_sources = []
        for el in self.news["source_link"]:
            list_sources.append((el,))
        list_tags = []
        if len(self.news["tags"]) >= 1:
            for el in self.news["tags"]:
                list_tags.append((el,))
        
        cursor.executemany("INSERT OR IGNORE INTO sources (name) VALUES(?)", list_sources)
        cursor.executemany("INSERT OR IGNORE INTO tags (name) VALUES(?)", list_tags)
        list_news = [
            ("TUT.BY", "blablablabla", 82934.4, "asdkkm.jpg", 3), 
            ("news2", "blablablabla2", 345435.2, "hasdoahd.jpg", 1)
        ]
        if len(self.news["img_link"]) >= 1:
            str_link = ", ".join(self.news["img_link"])
            news = (self.news["head"], self.news["text"], self.news["date"], str_link, self.news["rubric"])
        else:
            news = (self.news["head"], self.news["text"], self.news["date"], None, self.news["rubric"])
        cursor.execute("INSERT OR IGNORE INTO news (title, text, timestamp, img_link, rubric_id) VALUES(?, ?, ?, ?, (SELECT id FROM rubrics WHERE name = ?))", news)
        list_tags_news = []
        for el in self.news["tags"]:
            list_tags_news.append((el, self.news["head"], self.news["date"]))
        cursor.executemany("INSERT OR IGNORE INTO tags_news (id_tag, id_news) VALUES ((SELECT id FROM tags WHERE name = ?), (SELECT id FROM news WHERE title = ? and timestamp = ?))", list_tags_news)
        list_sources_news = []
        for el in self.news["source_link"]:
            list_sources_news.append((el, self.news["head"], self.news["date"]))
        cursor.executemany("INSERT OR IGNORE INTO sources_news (id_source, id_news) VALUES((SELECT id FROM sources WHERE name = ?), (SELECT id FROM news WHERE title = ? and timestamp = ?))", list_sources_news)
        connection.commit()
        connection.close()


if __name__ == "__main__":
    while True:
        str_date = ""
        path = os.path.join(BASE_DIR, 'save_date.txt')
        with open(path, 'r', encoding='utf-8') as f:
            str_date = f.readline()
        date = datetime.strptime(str_date, "%d.%m.%Y")
        now = datetime.now()
        str_temp_date = datetime.strftime(now, "%d.%m.%Y")
        temp_date = datetime.strptime(str_temp_date, "%d.%m.%Y")
        if date >= temp_date:
            print("Ошибка даты, попробуйте завтра!", "Дата для анализа: " + str_date, "Текущая дата: " + str_temp_date, sep="\n")
            break
        parser = Preview(page=str_date)
        parser.get_links()
        news = NewsParser()
        for pars in parser:
            news.__call__(pars)
        day_delta = timedelta(days=1)
        print(date, day_delta)
        date += day_delta
        str_date = datetime.strftime(date, "%d.%m.%Y")
        sleep_min = randint(1,30)
        print("\nСледующая дата(", str_date, ")проанализируется через", sleep_min, "минут\n")
        with open(path, 'w', encoding='utf-8') as f:
            f.write(str_date)
        time.sleep(60 * sleep_min)
    
    # parser = Preview(page="03.10.2000")
    # parser = Preview(page="19.01.2021")
    # parser.get_links()
    
    # print(parser[1])
    # print(parser[1:4])
    # print(parser[-5:-1:2])
    # print(parser["key"])
    # print(parser[3.4])
    
    # news = NewsParser()
    # for pars in parser:
    #     news.__call__(pars)
    
    # news = NewsParser()
    # news.__call__(parser[0])
    # pool = ThreadPoolExecutor(max_workers=1)
    # print(parser._Preview__links.__len__())
    # start = datetime.now()
    # time.sleep(2)
    # news_from_page = pool.map(news, parser)
    # for n in news_from_page:
        # pass
        # print(n)
        # print("=" * 150)
    # print(datetime.now() - start)
    # print(parser._Preview__links.__len__())

    # parser.save_to_json("03.10.2000")
    # parser.save_to_file("03.10.2000")
    # print(parser._Preview__links)