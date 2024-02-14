import logging
import os
import sys

from flask import Flask, make_response, request

SERVICE_HOST = os.getenv("SERVER_HOST", "0.0.0.0")
SERVICE_PORT = os.getenv("SERVER_PORT", 8080)


def get_logger(logger_name):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter("%(name)s [%(asctime)s] [%(levelname)s] %(message)s")
    )
    logger.addHandler(handler)
    return logger


logger = get_logger("echo-service")

app = Flask(__name__)


@app.get("/healthcheck")
def readiness_probe():
    return "I'm ready!"


@app.post("/echo")
def echo():
    """
    Main handler for input data sent by Snowflake.
    """
    message = request.json
    logger.debug("Received request: %s", message)

    if message is None or not message["data"]:
        logger.info("Received empty message")
        return {}

    # input format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...],
    #     ...
    #   ]}
    input_rows = message["data"]
    logger.info("Received %d rows", len(input_rows))

    # output format:
    #   {"data": [
    #     [row_index, column_1_value, column_2_value, ...}],
    #     ...
    #   ]}
    output_rows = [[row[0], get_echo_response(row[1])] for row in input_rows]
    logger.info("Produced %d rows", len(output_rows))

    response = make_response({"data": output_rows})
    response.headers["Content-type"] = "application/json"
    logger.debug("Sending response: %s", response.json)
    return response


def get_echo_response(inp):
    return f"I said {inp}"


if __name__ == "__main__":
    app.run(host=SERVICE_HOST, port=SERVICE_PORT)
