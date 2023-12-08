import pytest
from client import UDAClient, UDADatasetClient
from pydantic import BaseModel


class MockListData(BaseModel):
    shot: int
    signal_name: str


class MockUDASignal:
    pass


@pytest.fixture
def mocked_client(mocker):
    func = mocker.patch("client.UDAClient.__init__")
    func.return_value = None

    func = mocker.patch("client.UDAClient.get_signal")
    func.return_value = MockUDASignal()

    func = mocker.patch("client.UDAClient.get_signal_names")
    func.return_value = [
        MockListData(shot=30420, signal_name="ip"),
        MockListData(shot=30420, signal_name="ip2"),
    ]


def test_data_client_get_signal_names(mocked_client):
    client = UDADatasetClient()
    items = client.get_signal_metadata([30420])
    assert len(items) == 2


def test_data_client_get_signals(mocked_client):
    client = UDADatasetClient()
    for item in client.get_signals("ip", [30420, 30421]):
        assert item is not None
