from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple

import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream


class SourceFreeCurrencyApi(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [CurrencyConversion(api_key=config["api_key"], base_currency=config["base_currency"])]


class CurrencyConversion(HttpStream):
    url_base = "https://freecurrencyapi.net/"
    primary_key = None

    def __init__(self, api_key: str, base_currency: str, **kwargs):
        super().__init__(**kwargs)
        self.api_key = api_key
        self.base_currency = base_currency

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        pass

    def path(
        self, *,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
        **kwargs
    ) -> str:
        return f"api/v2/latest"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None,
    ) -> MutableMapping[str, Any]:
        return {"base_currency": self.base_currency, "apikey": self.api_key}

    def parse_response(
        self,
        response: requests.Response,
        *,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> Iterable[Mapping]:
        return [response.json()]
