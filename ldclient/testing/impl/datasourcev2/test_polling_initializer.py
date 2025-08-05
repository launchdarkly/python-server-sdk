# pylint: disable=missing-docstring

import json
from typing import Optional

from ldclient.impl.datasourcev2.polling import (
    PollingDataSource,
    PollingResult,
    Selector,
    polling_payload_to_changeset
)
from ldclient.impl.datasystem.protocolv2 import ChangeSetBuilder, IntentCode
from ldclient.impl.util import UnsuccessfulResponseException, _Fail, _Success


class MockExceptionThrowingPollingRequester:  # pylint: disable=too-few-public-methods
    def fetch(self, selector: Optional[Selector]) -> PollingResult:
        raise Exception("This is a mock exception for testing purposes.")


class MockPollingRequester:  # pylint: disable=too-few-public-methods
    def __init__(self, result: PollingResult):
        self._result = result

    def fetch(self, selector: Optional[Selector]) -> PollingResult:
        return self._result


def test_polling_has_a_name():
    mock_requester = MockPollingRequester(_Fail(error="failure message"))
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    assert ds.name() == "PollingDataSourceV2"


def test_error_is_returned_on_failure():
    mock_requester = MockPollingRequester(_Fail(error="failure message"))
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Fail)
    assert result.error == "failure message"
    assert result.exception is None


def test_error_is_recoverable():
    mock_requester = MockPollingRequester(
        _Fail(error="failure message", exception=UnsuccessfulResponseException(408))
    )
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Fail)
    assert result.error is not None
    assert result.error.startswith("Received HTTP error 408")
    assert isinstance(result.exception, UnsuccessfulResponseException)


def test_error_is_unrecoverable():
    mock_requester = MockPollingRequester(
        _Fail(error="failure message", exception=UnsuccessfulResponseException(401))
    )
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Fail)
    assert result.error is not None
    assert result.error.startswith("Received HTTP error 401")
    assert isinstance(result.exception, UnsuccessfulResponseException)


def test_handles_transfer_none():
    mock_requester = MockPollingRequester(
        _Success(value=(ChangeSetBuilder.no_changes(), {}))
    )
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Success)
    assert result.value is not None

    assert result.value.change_set.intent_code == IntentCode.TRANSFER_NONE
    assert result.value.change_set.changes == []
    assert result.value.persist is False


def test_handles_uncaught_exception():
    mock_requester = MockExceptionThrowingPollingRequester()
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Fail)
    assert result.error is not None
    assert (
        result.error
        == "Error: Exception encountered when updating flags. This is a mock exception for testing purposes."
    )
    assert isinstance(result.exception, Exception)


def test_handles_transfer_full():
    payload_str = '{"events":[ {"event":"server-intent","data":{"payloads":[ {"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":461,"intentCode":"xfer-full","reason":"payload-missing"}]}},{"event": "put-object","data": {"key":"sample-feature","kind":"flag","version":461,"object":{"key":"sample-feature","on":false,"prerequisites":[],"targets":[],"contextTargets":[],"rules":[],"fallthrough":{"variation":0},"offVariation":1,"variations":[true,false],"clientSideAvailability":{"usingMobileKey":false,"usingEnvironmentId":false},"clientSide":false,"salt":"9945e63a79a44787805b79728fee1926","trackEvents":false,"trackEventsFallthrough":false,"debugEventsUntilDate":null,"version":112,"deleted":false}}},{"event":"payload-transferred","data":{"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:461)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":461}}]}'
    change_set_result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(change_set_result, _Success)

    mock_requester = MockPollingRequester(_Success(value=(change_set_result.value, {})))
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Success)
    assert result.value is not None

    assert result.value.change_set.intent_code == IntentCode.TRANSFER_FULL
    assert len(result.value.change_set.changes) == 1
    assert result.value.persist is True


def test_handles_transfer_changes():
    payload_str = '{"events":[{"event": "server-intent","data": {"payloads":[{"id":"5A46PZ79FQ9D08YYKT79DECDNV","target":462,"intentCode":"xfer-changes","reason":"stale"}]}},{"event": "put-object","data": {"key":"sample-feature","kind":"flag","version":462,"object":{"key":"sample-feature","on":true,"prerequisites":[],"targets":[],"contextTargets":[],"rules":[],"fallthrough":{"variation":0},"offVariation":1,"variations":[true,false],"clientSideAvailability":{"usingMobileKey":false,"usingEnvironmentId":false},"clientSide":false,"salt":"9945e63a79a44787805b79728fee1926","trackEvents":false,"trackEventsFallthrough":false,"debugEventsUntilDate":null,"version":113,"deleted":false}}},{"event": "payload-transferred","data": {"state":"(p:5A46PZ79FQ9D08YYKT79DECDNV:462)","id":"5A46PZ79FQ9D08YYKT79DECDNV","version":462}}]}'
    change_set_result = polling_payload_to_changeset(json.loads(payload_str))
    assert isinstance(change_set_result, _Success)

    mock_requester = MockPollingRequester(_Success(value=(change_set_result.value, {})))
    ds = PollingDataSource(poll_interval=1.0, requester=mock_requester)

    result = ds.fetch()

    assert isinstance(result, _Success)
    assert result.value is not None

    assert result.value.change_set.intent_code == IntentCode.TRANSFER_CHANGES
    assert len(result.value.change_set.changes) == 1
    assert result.value.persist is True
