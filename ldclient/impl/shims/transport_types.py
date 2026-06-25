"""
Shared types for the async/sync transport shims.
"""

from typing import Mapping


class TransportResponse:
    """A minimal uniform HTTP response: status code, headers, and decoded body.

    ``headers`` is whatever case-insensitive mapping the underlying HTTP
    library produced, so lookups like ``headers.get('ETag')`` work regardless
    of the casing the server used.
    """

    def __init__(self, status: int, headers: Mapping[str, str], body: str):
        self.status = status
        self.headers = headers
        self.body = body
