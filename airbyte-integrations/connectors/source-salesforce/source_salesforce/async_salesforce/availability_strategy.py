#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
import typing
from typing import Optional, Tuple

from airbyte_cdk.sources.async_cdk.streams.http.availability_strategy_async import AsyncHttpAvailabilityStrategy
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http.utils import HttpError
from requests import codes

if typing.TYPE_CHECKING:
    from airbyte_cdk.sources import Source


class AsyncSalesforceAvailabilityStrategy(AsyncHttpAvailabilityStrategy):
    def handle_http_error(
        self, stream: Stream, logger: logging.Logger, source: Optional["Source"], error: HttpError
    ) -> Tuple[bool, Optional[str]]:
        """
        There are several types of Salesforce sobjects that require additional processing:
          1. Sobjects for which the user, after setting up the data using Airbyte, restricted access,
             and we will receive 403 HTTP errors.
          2. There are streams that do not allow you to make a sample using Salesforce `query` or `queryAll`.
             And since we use a dynamic method of generating streams for Salesforce connector - at the stage of discover,
             we cannot filter out these streams, so we check for them before reading from the streams.
        """
        if error.status_code in [codes.FORBIDDEN, codes.BAD_REQUEST]:
            error_data = error.json()[0]
            error_code = error_data.get("errorCode", "")
            if error_code != "REQUEST_LIMIT_EXCEEDED" or error_code == "INVALID_TYPE_FOR_OPERATION":
                return False, f"Cannot receive data for stream '{stream.name}', error message: '{error_data.get('message')}'"
            return True, None
        raise error