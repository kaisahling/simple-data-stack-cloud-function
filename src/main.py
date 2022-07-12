import os

from flask import jsonify, abort

from utils import load_data, transform_data, write_data, row_gen

TWITTER_ID = os.getenv("TWITTER_ID", default="https://twitter.com/elonmusk")


def main(request):

    try:
        data = load_data(TWITTER_ID)

        transformed_data = transform_data(data)

        rows = row_gen(transformed_data)
        list(map(write_data, rows))

        return jsonify(status="success"), 200

    except Exception as error:
        error_message = f"An error occurred: {error}"
        print(error_message)
        return abort(500)
