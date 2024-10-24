from typing import Optional

import pytest

from ldclient.impl.http import _get_proxy_url


@pytest.mark.parametrize(
    'target_uri, no_proxy, expected',
    [
        ('https://secure.example.com', '', 'https://secure.proxy:1234'),
        ('http://insecure.example.com', '', 'http://insecure.proxy:6789'),
        ('https://secure.example.com', 'secure.example.com', None),
        ('https://secure.example.com', 'secure.example.com:443', None),
        ('https://secure.example.com', 'secure.example.com:80', 'https://secure.proxy:1234'),
        ('https://secure.example.com', 'wrong.example.com', 'https://secure.proxy:1234'),
        ('https://secure.example.com:8080', 'secure.example.com', None),
        ('https://secure.example.com:8080', 'secure.example.com:443', 'https://secure.proxy:1234'),
        ('https://secure.example.com:8080', 'secure.example.com:443,', 'https://secure.proxy:1234'),
        ('https://secure.example.com:8080', 'secure.example.com:443,,', 'https://secure.proxy:1234'),
        ('https://secure.example.com:8080', ':8080', 'https://secure.proxy:1234'),
        ('https://secure.example.com', 'example.com', None),
        ('https://secure.example.com', 'example.com:443', None),
        ('https://secure.example.com', 'example.com:80', 'https://secure.proxy:1234'),
        ('http://insecure.example.com', 'insecure.example.com', None),
        ('http://insecure.example.com', 'insecure.example.com:443', 'http://insecure.proxy:6789'),
        ('http://insecure.example.com', 'insecure.example.com:80', None),
        ('http://insecure.example.com', 'wrong.example.com', 'http://insecure.proxy:6789'),
        ('http://insecure.example.com:8080', 'secure.example.com', None),
        ('http://insecure.example.com:8080', 'secure.example.com:443', 'http://insecure.proxy:6789'),
        ('http://insecure.example.com', 'example.com', None),
        ('http://insecure.example.com', 'example.com:443', 'http://insecure.proxy:6789'),
        ('http://insecure.example.com', 'example.com:80', None),
        ('secure.example.com', 'secure.example.com', None),
        ('secure.example.com', 'secure.example.com:443', 'http://insecure.proxy:6789'),
        ('secure.example.com', 'secure.example.com:80', None),
        ('secure.example.com', 'wrong.example.com', 'http://insecure.proxy:6789'),
        ('secure.example.com:8080', 'secure.example.com', None),
        ('secure.example.com:8080', 'secure.example.com:80', 'http://insecure.proxy:6789'),
        ('https://secure.example.com', '*', None),
        ('https://secure.example.com:8080', '*', None),
        ('http://insecure.example.com', '*', None),
        ('http://insecure.example.com:8080', '*', None),
        ('secure.example.com:443', '*', None),
        ('insecure.example.com:8080', '*', None),
    ],
)
def test_honors_no_proxy(target_uri: str, no_proxy: str, expected: Optional[str], monkeypatch):
    monkeypatch.setenv('https_proxy', 'https://secure.proxy:1234')
    monkeypatch.setenv('http_proxy', 'http://insecure.proxy:6789')
    monkeypatch.setenv('no_proxy', no_proxy)

    proxy_url = _get_proxy_url(target_uri)

    assert proxy_url == expected
