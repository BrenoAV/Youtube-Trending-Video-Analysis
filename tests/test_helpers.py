"""
Test of helpers

Module to test functionalities of the helper functions

Author: brenoAV
Last Modified: 11-21-2023
"""
import json
from tempfile import NamedTemporaryFile

import pytest
from pyspark.sql import DataFrame, SparkSession

from src.helpers import (
    get_channel_title_by_id,
    get_map_category_name_by_id,
    title_tokenize,
)


@pytest.fixture(scope="session")
def map_id_title() -> DataFrame:
    spark = SparkSession.builder.appName("Test of df helpers").getOrCreate()
    data = [("idch1", "titlech1"), ("idch2", "titlech2"), ("idch3", "titlech3")]
    columns = ["channelId", "channelTitle"]
    return spark.createDataFrame(data).toDF(*columns)


@pytest.fixture(scope="session")
def map_id_title_wrong_cols() -> DataFrame:
    spark = SparkSession.builder.appName("Test of df helpers").getOrCreate()
    data = [("idch1", "titlech1"), ("idch2", "titlech2"), ("idch3", "titlech3")]
    columns = ["channel_id", "channel_title"]
    return spark.createDataFrame(data).toDF(*columns)


@pytest.mark.parametrize(
    "channel_id, channel_title",
    [("idch3", "titlech3"), ("idch1", "titlech1"), ("idch2", "titlech2")],
)
def test_get_title_channel_by_id_correct_id(map_id_title, channel_id, channel_title):
    # Checking if return the correct title if correct
    assert get_channel_title_by_id(channel_id, map_id_title) == channel_title


def test_get_title_channel_by_id_wrong_id(map_id_title):
    # Checking if returns a None type for invalid
    channel_id = "invalid_id"
    assert get_channel_title_by_id(channel_id, map_id_title) == ""


def test_get_title_channel_by_id_wrong_columns(map_id_title_wrong_cols):
    # AssertionError when the columns is not correct
    with pytest.raises(AssertionError) as execinfo:
        _ = get_channel_title_by_id("idch1", map_id_title_wrong_cols)

    assert (
        "the map_id_title parameter must have the columns: `channelId` and `channelTitle`"
        in str(execinfo.value)
    )


def test_get_map_category_name_by_id():
    data = {
        "items": [
            {"id": "1", "snippet": {"title": "Film & Animation"}},
            {"id": "2", "snippet": {"title": "Autos & Vehicles"}},
            {"id": "10", "snippet": {"title": "Music"}},
        ]
    }
    expected_map_category_name_id = {
        "10": "Music",
        "2": "Autos & Vehicles",
        "1": "Film & Animation",
    }
    with NamedTemporaryFile(
        suffix=".json", mode="w", encoding="utf-8", delete=True
    ) as file:
        file.write(json.dumps(data))
        file.seek(0)

        assert get_map_category_name_by_id(file.name) == expected_map_category_name_id


def test_get_map_category_name_by_id_invalid():
    data = {
        "no_valid": [
            {"id": "1", "snippet": {"title": "Film & Animation"}},
            {"id": "2", "snippet": {"title": "Autos & Vehicles"}},
            {"id": "10", "snippet": {"title": "Music"}},
        ]
    `date +%Y-%m-%d`}

    with NamedTemporaryFile(
        suffix=".json", mode="w", encoding="utf-8", delete=True
    ) as file:
        file.write(json.dumps(data))
        file.seek(0)

        with pytest.raises(KeyError):
            get_map_category_name_by_id(file.name)


def test_clean_title():
    title = "França 14 x 0 Gibaltrar pelas Eliminatórias da Euro: melhores \
momentos do show de Mbappé e companhia"
    expected_title = [
        "frança",
        "x",
        "gibaltrar",
        "eliminatórias",
        "euro",
        "melhores",
        "momentos",
        "show",
        "mbappé",
        "companhia",
    ]
    assert title_tokenize(title) == expected_title
