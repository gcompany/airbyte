import pytest
from unittest.mock import Mock, call
from unittest import mock

from airbyte_cdk.models import SyncMode
from .common import MockGoogleAdsClient as MockGoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.ads.googleads.v6.errors.types.errors import ErrorCode, GoogleAdsError, GoogleAdsFailure
from google.ads.googleads.v6.errors.types.request_error import RequestErrorEnum
from grpc import RpcError
from source_google_ads.google_ads import GoogleAds
from source_google_ads.streams import ClickView


@pytest.fixture(scope="module")
def test_config():
    config = {
        "credentials": {
            "developer_token": "test_token",
            "client_id": "test_client_id",
            "client_secret": "test_client_secret",
            "refresh_token": "test_refresh_token",
        },
        "customer_id": "123",
        "start_date": "2021-01-01",
        "conversion_window_days": 14,
    }
    return config


# class MockErrorResponse:
#     def __iter__(self):
#         return self
#
#     def __next__(self):
#         e = GoogleAdsException(
#             error=RpcError(),
#             failure=GoogleAdsFailure(
#                 errors=[GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]
#             ),
#             call=RpcError(),
#             request_id="test",
#         )
#         raise e


# class MockGoogleAdsService:
#     count = 0
#
#     def search(self, search_request):
#         self.count += 1
#         if self.count == 1:
#             # For the first attempt (with date range = 15 days) return Error Response
#             return MockErrorResponse()
#         else:
#             # the second attempt should succeed, (date range = 7 days)
#             # this payload is dummy, in this case test stream records will be printed with None values in all fields.
#             return [{"id": 1}, {"id": 2}]


# class MockGoogleAdsServiceWhichFails:
#     def search(self, search_request):
#         # For all attempts, return Error Response
#         return MockErrorResponse()


# class MockGoogleAdsClient(MockGoogleAdsClientBase):
#     pass

    # def get_service(self, service):
    #     return MockGoogleAdsService()


# class MockGoogleAdsClientWhichFails(MockGoogleAdsClientBase):
#     pass
    # def get_service(self, service):
    #     return MockGoogleAdsServiceWhichFails()


@pytest.fixture
def mock_ads_client(mocker):
    """Mock google ads library method, so it returns mocked Client"""
    mocker.patch("source_google_ads.google_ads.GoogleAdsClient.load_from_dict", return_value=MockGoogleAdsClient(test_config))


def mock_response():
    yield from [
        {"segments.date": "2021-01-01", "click_view.gclid": "1"},
        {"segments.date": "2021-01-02", "click_view.gclid": "2"},
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]

    e = GoogleAdsException(
        error=RpcError(),
        failure=GoogleAdsFailure(
            errors=[
                GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]
        ),
        call=RpcError(),
        request_id="test",
    )
    raise e


def mock_response_2():
    yield from [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
        {"segments.date": "2021-01-03", "click_view.gclid": "5"},
        {"segments.date": "2021-01-04", "click_view.gclid": "6"},
        {"segments.date": "2021-01-05", "click_view.gclid": "7"},
    ]


class MockGoogleAds(GoogleAds):
    count = 0

    def parse_single_result(self, schema, result):
        return result

    def send_request(self, query: str):
        self.count += 1
        if self.count == 1:
            return mock_response()
        else:
            return mock_response_2()


@pytest.fixture
def mock_ads_client_which_fails(mocker, test_config):
    mocker.patch("source_google_ads.google_ads.GoogleAdsClient.load_from_dict", return_value=MockGoogleAdsClientWhichFails(test_config))
    # mocker.patch("source_google_ads.google_ads.GoogleAds",
    #              return_value=MockGoogleAds(credentials=test_config["credentials"], customer_id=test_config["customer_id"]))


def test_page_token_expired_retry_succeeds(mock_ads_client, test_config):
    """
    Page token expired while reading records on date 2021-01-03
    It should retry reading starting from 2021-01-03
    It shouldn't read records on 2021-01-01, 2021-01-02
    """
    stream_slice = {
        "start_date": "2021-01-01",
        "end_date": "2021-01-10"
    }

    google_api = MockGoogleAds(credentials=test_config["credentials"], customer_id=test_config["customer_id"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=test_config["conversion_window_days"],
        start_date=test_config["start_date"],
        time_zone="local",
        end_date="2021-04-04",
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    result = list(stream.read_records(
        sync_mode=SyncMode.incremental,
        cursor_field=["segments.date"],
        stream_slice=stream_slice)
    )
    assert len(result) == 9
    print(result)

    calls = [call({"start_date": "2021-01-01", "end_date": "2021-01-10"}),
             call({"start_date": "2021-01-03", "end_date": "2021-01-10"})]
    stream.get_query.assert_has_calls(calls, any_order=False)

    # stream.get_query.assert_any_call({"start_date": "2021-01-01", "end_date": "2021-01-10"})

    assert stream.get_query.call_count == 2
    # assert stream.get_query.call_args_list == [{"start_date": "2021-01-01", "end_date": "2021-01-10"},
    #                                            {"start_date": "2021-01-03", "end_date": "2021-01-10"}]
    # stream.get_query.assert_called_with({
    #     "start_date": "2021-01-03",
    #     "end_date": "2021-01-10"
    # })


# =========================

def mock_response_fails():
    yield from [
        {"segments.date": "2021-01-01", "click_view.gclid": "1"},
        {"segments.date": "2021-01-02", "click_view.gclid": "2"},

        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]

    e = GoogleAdsException(
        error=RpcError(),
        failure=GoogleAdsFailure(
            errors=[
                GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]
        ),
        call=RpcError(),
        request_id="test",
    )
    raise e


def mock_response_fails_2():
    yield from [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]

    e = GoogleAdsException(
        error=RpcError(),
        failure=GoogleAdsFailure(
            errors=[
                GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]
        ),
        call=RpcError(),
        request_id="test",
    )
    raise e


class MockGoogleAdsFails(GoogleAds):
    count = 0

    def parse_single_result(self, schema, result):
        return result

    def send_request(self, query: str):
        self.count += 1
        if self.count == 1:
            return mock_response_fails()
        else:
            return mock_response_fails_2()


def test_page_token_expired_retry_fails(mock_ads_client, test_config):
    """
    Page token expired while reading records within date "2021-01-03", it should raise error,
    because Google Ads API doesn't allow filter by datetime.
    """
    stream_slice = {
        "start_date": "2021-01-01",
        "end_date": "2021-01-10"
    }

    google_api = MockGoogleAdsFails(credentials=test_config["credentials"], customer_id=test_config["customer_id"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=test_config["conversion_window_days"],
        start_date=test_config["start_date"],
        time_zone="local",
        end_date="2021-04-04",
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    with pytest.raises(GoogleAdsException):
        list(stream.read_records(
            sync_mode=SyncMode.incremental,
            cursor_field=["segments.date"],
            stream_slice=stream_slice)
        )

    stream.get_query.assert_called_with({
        "start_date": "2021-01-03",
        "end_date": "2021-01-10"
    })
    assert stream.get_query.call_count == 2


# =======================


def mock_response_fails_one_date():
    yield from [
        {"segments.date": "2021-01-03", "click_view.gclid": "3"},
        {"segments.date": "2021-01-03", "click_view.gclid": "4"},
    ]

    e = GoogleAdsException(
        error=RpcError(),
        failure=GoogleAdsFailure(
            errors=[
                GoogleAdsError(error_code=ErrorCode(request_error=RequestErrorEnum.RequestError.EXPIRED_PAGE_TOKEN))]
        ),
        call=RpcError(),
        request_id="test",
    )
    raise e


class MockGoogleAdsFailsOneDate(GoogleAds):
    def parse_single_result(self, schema, result):
        return result

    def send_request(self, query: str):
        return mock_response_fails_one_date()


def test_page_token_expired_it_should_fail_date_range_1_day(mock_ads_client, test_config):
    """
    Page token expired while reading records within date "2021-01-03",
    it should raise error, because Google Ads API doesn't allow filter by datetime.
    Minimum date range is 1 day.
    """
    stream_slice = {
        "start_date": "2021-01-03",
        "end_date": "2021-01-04"
    }

    google_api = MockGoogleAdsFailsOneDate(credentials=test_config["credentials"], customer_id=test_config["customer_id"])
    incremental_stream_config = dict(
        api=google_api,
        conversion_window_days=test_config["conversion_window_days"],
        start_date=test_config["start_date"],
        time_zone="local",
        end_date="2021-04-04",
    )
    stream = ClickView(**incremental_stream_config)
    stream.get_query = Mock()
    stream.get_query.return_value = "query"

    with pytest.raises(GoogleAdsException):
        list(stream.read_records(
            sync_mode=SyncMode.incremental,
            cursor_field=["segments.date"],
            stream_slice=stream_slice)
        )

    stream.get_query.assert_called_with({
        "start_date": "2021-01-03",
        "end_date": "2021-01-04"
    })
    assert stream.get_query.call_count == 1
