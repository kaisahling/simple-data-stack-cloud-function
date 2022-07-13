import os
from datetime import datetime, timedelta
import requests
from pandas import DataFrame
from google.cloud import storage


BEARER_TOKEN = os.getenv("BEARER_TOKEN")
BUCKET =  os.getenv("STORAGE_BUCKET")

STORAGE_CLIENT = storage.Client()

NOW = datetime.utcnow()
now_str = NOW.strftime("%Y-%m-%dT%H:%M:%S")


def create_url(user_id: str) -> str:
    """
    Create Twitter api url with query parameters
    :param user_id: Twitter user id
    :return:
    """
    # each day, we load tweets from last 3 days so that we can track how tweet metrics increase over time
    start_time = (NOW - timedelta(days=3)).strftime(format="%Y-%m-%dT%H:%M:%S.000Z")
    end_time = NOW.strftime(format="%Y-%m-%dT%H:%M:%S.000Z")
    return "https://api.twitter.com/2/users/{}/tweets?start_time={}&end_time={}".format(
        user_id, start_time, end_time
    )


def get_params() -> dict:
    # Tweet fields are adjustable.
    # Options include:
    # attachments, author_id, context_annotations,
    # conversation_id, created_at, entities, geo, id,
    # in_reply_to_user_id, lang, non_public_metrics, organic_metrics,
    # possibly_sensitive, promoted_metrics, public_metrics, referenced_tweets,
    # source, text, and withheld
    return {
        "tweet.fields": "created_at,author_id,public_metrics,referenced_tweets,in_reply_to_user_id,context_annotations"
    }


def bearer_oauth(r):
    """
     Method required by bearer token authentication.
    :param r:
    :return:
    """
    r.headers["Authorization"] = f"Bearer {BEARER_TOKEN}"
    r.headers["User-Agent"] = "v2UserTweetsPython"
    return r


def connect_to_endpoint(url: str, params: dict) -> dict:
    """
    Connects to Twitter API endpoint and fetch data
    :param url: Endpoint url
    :param params: Request body
    :return:
    """
    response = requests.request("GET", url, auth=bearer_oauth, params=params)
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )
    return response.json()


def load_data(twitter_id: str) -> dict:
    """
    Loads data from Twitter api
    :param twitter_id: twitter id used for call
    :return:
    """
    url = create_url(twitter_id)
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    return json_response


def transform_data(data: dict) -> DataFrame:
    """
    Transform json response to pandas dataframe
    :param data: json response from Twitter api
    :return:
    """
    df = DataFrame(data["data"])
    if "context_annotations" in df.columns:
        df = df.astype({"context_annotations": str})
    else:
        df["context_annotations"] = None
    return df


def write_data(row: tuple) -> None:
    """
    Write dataframe row as json to Cloud Storage bucket
    :param row: a dataframe's row
    :return:
    """
    bucket = STORAGE_CLIENT.get_bucket(BUCKET)
    json_name = "tweet-{}.json".format(row[1])
    bucket.blob(json_name).upload_from_string(row[0], "text/json")


def row_gen(df: DataFrame):
    """
    Creates a generator returning a dataframe's rows
    :param df: dataframe the generator is based on
    :return:
    """
    for i in range(len(df)):
        yield df.iloc[i : i + 1].to_json(orient="records", lines=True), df.iloc[
            i : i + 1
        ]["id"].iloc[0]
