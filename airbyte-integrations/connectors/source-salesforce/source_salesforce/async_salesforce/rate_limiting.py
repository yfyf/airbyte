#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import sys

import aiohttp
import backoff
from airbyte_cdk.sources.async_cdk.streams.http.exceptions_async import AsyncDefaultBackoffException
from requests import codes, exceptions  # type: ignore[import]

TRANSIENT_EXCEPTIONS = (
    AsyncDefaultBackoffException,
    aiohttp.ClientPayloadError,
    aiohttp.ServerTimeoutError,
    aiohttp.ServerConnectionError,
    aiohttp.ServerDisconnectedError,
)

logger = logging.getLogger("airbyte")


def default_backoff_handler(max_tries: int, factor: int, **kwargs):
    def log_retry_attempt(details):
        _, exc, _ = sys.exc_info()
        logger.info(str(exc))
        logger.info(f"Caught retryable error after {details['tries']} tries. Waiting {details['wait']} seconds then retrying...")

    def should_give_up(exc):
        give_up = exc.response is not None and exc.response.status_code != codes.too_many_requests and 400 <= exc.status_code < 500

        # Salesforce can return an error with a limit using a 403 code error.
        if exc.status_code == codes.forbidden:
            error_data = exc.json()[0]
            if error_data.get("errorCode", "") == "REQUEST_LIMIT_EXCEEDED":
                give_up = True

        if give_up:
            logger.info(f"Giving up for returned HTTP status: {exc.status_code}, body: {exc.text}")
        return give_up

    return backoff.on_exception(
        backoff.expo,
        TRANSIENT_EXCEPTIONS,
        jitter=None,
        on_backoff=log_retry_attempt,
        giveup=should_give_up,
        max_tries=max_tries,
        factor=factor,
        **kwargs,
    )