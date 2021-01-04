import os
import json
import pickle
import requests
from datetime import datetime
from bs4 import BeautifulSoup as BS
from abc import ABC, abstractmethod

from fxp.parser import HOST, PREVIEW_URL, BASE_DIR

class BaseParser(ABC):
    def __init__(self, user_agent:str = None):
        pass
        # self._user_agent = user_agent if user_agent is not None else DEFAULT_USER_AGENT

    def _get_page(self, url):
        # response = requests.get(
        #     url, 
        #     headers={"User-Agent": self._user_agent}
        # )
        response = requests.get(url)
        if response.status_code == 200:
            return BS(response.text, features="html.parser")
        raise ValueError("Response not 200")

    # @abstractmethod
    # def save_to_file(self, name: str) -> None:
    #     """Save news to file

    #     Args:
    #         name (str): file name
    #     """

    # def save_to_json(self, path: str) -> None:
    #     """Save news to json file

    #     Args:
    #         path (str): file_name
    #     """

class Preview(BaseParser):
    def __init__(self, **kwargs):
        # super().__init__(kwargs.get("user_agent"))
        now = datetime.now()
        temp_date = now.strftime("%d.%m.%Y")
        self.__num_page = kwargs.get("page") or temp_date
        self.__links = []

    def get_links(self):
        try:
            html = self._get_page(PREVIEW_URL.format(HOST, self.__num_page))
            print(html)
        except ValueError as error:
            print(error)
            self.__links = []
        # else:
        #     box = html.find()


if __name__ == "__main__":
    parser = Preview(page="03.10.2000")
    parser.get_links()