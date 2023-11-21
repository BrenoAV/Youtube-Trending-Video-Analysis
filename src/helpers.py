"""
Helpers

This module provide basic tasks to work with YouTube data

Author: brenoAV
Last Modified: 11-21-2023
"""
import json

import nltk
from nltk.tokenize import word_tokenize
from pyspark.sql import DataFrame

nltk.download("punkt", quiet=True)


def get_channel_title_by_id(channel_id: str, map_id_title: DataFrame) -> str:
    """Get the channel's title using the ID information.

    Expected a pyspark.sql.DataFrame as:

    | channelId | channelTitle |
    | --------- | ------------ |
    | id1       | title1       |
    | ...       | ...          |

    Args:
        channel_id (str): a string that contain the valid ID
        map_id_title (DataFrame): a dataframe with two columns (channelId, channelTitle)

    Returns:
        str: title of the YouTube channel or empty string if invalid
    """
    assert set(map_id_title.columns) == {
        "channelId",
        "channelTitle",
    }, "the map_id_title parameter must have the columns: `channelId` and `channelTitle`"
    item_founded = map_id_title.filter(map_id_title["channelId"] == channel_id).first()
    if item_founded:
        return item_founded.asDict()["channelTitle"]
    return ""


def get_map_category_name_by_id(filename: str) -> dict:
    """Generate a dictionary which the keys is the YouTube categoryId and the
    value is the categories name

    Args:
        filename (str): a json file with the category id

    Returns:
        dict: keys are the ids and the values are the categories name
    """
    with open(filename, mode="r", encoding="utf-8") as file:
        data = json.load(file)

    map_category_name_by_id = {}
    try:
        for item in data["items"]:
            map_category_name_by_id[item["id"]] = item["snippet"]["title"]
    except KeyError as exc:
        raise KeyError("invalid file format") from exc

    return map_category_name_by_id


def title_tokenize(title: str) -> list[str]:
    """Generate a list with tokens extracted from the title (string)

    Args:
        title (str): the video title

    Returns:
        list[str]: list with the tokens extracted from the title
    """
    tokens = word_tokenize(title, language="portuguese")
    tokens = list(map(lambda x: x.lower().strip(), tokens))
    tokens = list(filter(lambda x: x.isalpha() or (x in ["!", "?"]), tokens))
    # fmt: off
    stop_words = ['a', 'à', 'ao', 'aos', 'as', 'às', 'da', 'com'
                  'das', 'de', 'do', 'dos', 'e', 'é', 'em',
                  'entre', 'era', 'eram', 'éramos', 'essa',
                  'essas', 'esse', 'esses', 'esta', 'está',
                  'estamos', 'estão', 'estar', 'estas', 'estava',
                  'estavam', 'estávamos', 'este', 'esteja', 'estejam',
                  'estejamos', 'estes', 'esteve', 'estive', 'estivemos',
                  'estiver', 'estivera', 'estiveram', 'estivéramos', 'estiverem',
                  'estivermos', 'estivesse', 'estivessem', 'estivéssemos',
                  'fôramos', 'forem', 'formos', 'fosse', 'fossem', 'fôssemos',
                  'há', 'haja', 'hajam', 'hajamos', 'hão', 'havemos', 'haver',
                  'hei', 'houve', 'houvemos', 'houver', 'houvera', 'houverá',
                  'houveram', 'houvéramos', 'houverão', 'houverei', 'houverem',
                  'houveremos', 'houveria', 'houveriam', 'houveríamos', 'houvermos',
                  'houvesse', 'houvessem', 'houvéssemos', 'isso', 'isto', 'lhe', 'lhes',
                  'me', 'mesmo', 'na', 'nas', 'nem', 'no', 'nos', 'nós', 'num', 'numa',
                  'o', 'os', 'ou', 'para', 'pela', 'pelas', 'pelo', 'pelos', 'por',
                  'qual', 'quando', 'que', 'quem', 'são', 'se', 'seja', 'sejam',
                  'sejamos', 'sem', 'ser', 'será', 'serão', 'serei', 'seremos', 'seria',
                  'seriam', 'seríamos', 'seu', 'seus', 'só', 'somos', 'te', 'tem',
                  'tém', 'terá', 'terão', 'terei', 'teremos', 'teria', 'teriam',
                  'teríamos', 'teu', 'teus', 'teve', 'tinha', 'tinham', 'tínhamos',
                  'tive', 'tivemos', 'tiver', 'tivera', 'tiveram', 'tivéramos',
                  'tiverem', 'tivermos', 'tivesse', 'tivessem', 'tivéssemos', 'tu',
                  'tua', 'tuas', 'um', 'uma', 'você', 'vocês', 'vos', 'com', 'mais',
                  "the", "não", "como", "of", "foi", "fiz", "sobre", "mas", "após",
                  "pra", "pro", "faz", "vai", "tudo"]
    # fmt: on
    tokens = list(filter(lambda word: word not in stop_words, tokens))
    return tokens
