import os
from datetime import datetime, timedelta

from pandas import DataFrame
from google.cloud import storage

DIRECTORY = os.getenv("SINK_DIRECTORY")

BEARER_TOKEN = os.getenv("BEARER_TOKEN")

STORAGE_CLIENT = storage.Client()

NOW = datetime.utcnow()
now_str = NOW.strftime("%Y-%m-%dT%H:%M:%S")


def create_url(user_id: str):
    """

    :param user_id:
    :return:
    """
    # each day, we load tweets from last 3 days so that we can track how tweet metrics increase over time
    start_time = (NOW - timedelta(days=3)).strftime(format="%Y-%m-%dT%H:%M:%S.000Z")
    end_time = NOW.strftime(format="%Y-%m-%dT%H:%M:%S.000Z")
    return "https://api.twitter.com/2/users/{}/tweets?start_time={}&end_time={}".format(
        user_id, start_time, end_time
    )


def get_params():
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


def connect_to_endpoint(url, params):
    """

    :param url:
    :param params:
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


def load_data(twitter_id: str):
    """

    :param twitter_id:
    :return:
    """
    url = create_url(twitter_id)
    params = get_params()
    json_response = connect_to_endpoint(url, params)
    return json_response


def transform_data(data: dict, person_id: int, company_id: int):
    """

    :param data:
    :param person_id:
    :param company_id:
    :return:
    """
    df = DataFrame(data["data"])
    if "context_annotations" in df.columns:
        df = df.astype({"context_annotations": str})
    else:
        df["context_annotations"] = None
    return df


def write_data(row: tuple):
    """

    :param row:
    :return:
    """
    bucket = STORAGE_CLIENT.get_bucket(os.getenv("SINK"))
    json_name = "{}tweet-{}.json".format(DIRECTORY, row[1])
    bucket.blob(json_name).upload_from_string(row[0], "text/json")


def row_gen(df):
    for i in range(len(df)):
        yield df.iloc[i : i + 1].to_json(orient="records", lines=True), df.iloc[
            i : i + 1
        ]["id"].iloc[0]
