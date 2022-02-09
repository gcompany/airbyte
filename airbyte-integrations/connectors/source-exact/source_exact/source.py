from abc import abstractmethod
from typing import Any, Iterable, List, Mapping, Optional, Tuple

import requests
import psycopg2
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.sources.streams.http.auth import Oauth2Authenticator


class SourceExact(AbstractSource):
    def get_refresh_token(self) -> str:
        """
        1. Get a refresh token from a static location
        2. Check if this refresh token is valid by calling an endpoint
            2.1 If the refresh token is invalid, generate a new one and replace it in the static location
            2.2 If refresh fails check if there is a new token in static location and retry (in case of 2 simultaneous refreshes)
        3. return valid refresh token

        get airbyte-db ip (host) by using:
        docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' airbyte-db
        """
        conn = psycopg2.connect(dbname="airbyte", user="docker", password="docker", host="172.18.0.4", port="5432")
        cur = conn.cursor()
        cur.execute("SELECT * FROM airbyte_metadata;")
        result = cur.fetchall()
        print(result)
        return "refresh-token"

    def get_authenticator(self, config):
        refresh_token = self.get_refresh_token()

        return Oauth2Authenticator(
            token_refresh_endpoint=config["token_refresh_endpoint"],
            client_id=config["client_id"],
            client_secret=config["client_secret"],
            refresh_token=refresh_token,
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        authenticator = self.get_authenticator(config=config)
        return [
            CashflowPayments(authenticator=authenticator),
            CashflowReceivables(authenticator=authenticator),
        ]


class ExactHttpStream(HttpStream):
    url_base = "<BASE_URL>"
    primary_key = None

    @property
    @abstractmethod
    def _path(self):
        pass

    def path(self, **kwargs) -> str:
        return self._path

    def next_page_token(
            self,
            response: requests.Response,
    ) -> Optional[Mapping[str, Any]]:
        pass

    def parse_response(
            self,
            response: requests.Response,
            *,
            stream_state: Mapping[str, Any],
            stream_slice: Mapping[str, Any] = None,
            next_page_token: Mapping[str, Any] = None,
    ) -> Iterable[Mapping]:
        pass


class IncrementalExactHttpStream(ExactHttpStream):
    _path = None


"""the only thing you need to change is the path with a good REST set-up"""


class CashflowPayments(IncrementalExactHttpStream):
    _path = "/bulk/Cashflow/Payments"


class CashflowReceivables(IncrementalExactHttpStream):
    _path = "/bulk/Cashflow/Receivables"
