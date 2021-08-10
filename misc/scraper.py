import re

from bs4 import BeautifulSoup
import requests

session = requests.Session()


def get_director_genre_production(link):
    dict_data = {}

    global session
    content = session.get(link)

    table_soup = BeautifulSoup(content.content, "html.parser")

    tables = table_soup.find("div", attrs={"id": "summary"})
    # print(table)
    rows = (tables.find_all("table"))
    cast_crew = link.replace("=summary", "=cast-and-crew")
    cast_tables = session.get(cast_crew)

    cast_soup = BeautifulSoup(cast_tables.content, "html.parser")

    cast_div = cast_soup.find_all("div", {"class": "cast_new"})
    cast_table = cast_div[2].find("table")
    director = cast_table.find_all("tr")[0].text.split("\n")
    index = director.index("Director")
    director_name = director[index - 2]

    for tr in rows[4].find_all("tr"):
        td = tr.text.split("\n")
        if any(["Genre" in data for data in td]):
            dict_data["Genre"] = " ".join(td).split(":")[-1]
        elif any(["Production Companies" in data for data in td]):
            dict_data["Production Company"] = " ".join(td).split(":")[-1]

        # elif any([""])
    dict_data["Director name"] = director_name
    return dict_data


# print(get_director_genre_production("https://www.the-numbers.com/movie/Star-Wars-Ep-VII-The-Force-Awakens#tab=summary"))
def get_movie_name(link):
    # https://www.the-numbers.com/movie/Star-Wars-Ep-VII-The-Force-Awakens#tab=summary
    pattern = re.compile("/[A-Za-z0-9-()]+#")
    x = re.search(pattern, link)
    start = x.start() + 1
    stop = x.end() - 1

    return link[start:stop]


print(get_director_genre_production("https://www.the-numbers.com/movie/Star-Wars-Ep-VII-The-Force-Awakens#tab=summary"))