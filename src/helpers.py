"""
Helpers

This module provide basic tasks to work with YouTube data

Author: brenoAV
Last Modified: 11-18-2023
"""
import json

import requests
from pyspark.sql import DataFrame


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


def get_channel_info_by_id(channel_id: str, key: str) -> str:
    params = {"id": channel_id, "part": "snippet,statistics,status", "key": key}
    url = "https://www.googleapis.com/youtube/v3/channels"
    r = requests.get(url=url, params=params, timeout=5)
    return r.json()
