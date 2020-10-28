"""
Server-Sent Events implementation for streaming.

Based on: https://bitbucket.org/btubbs/sseclient/src/a47a380a3d7182a205c0f1d5eb470013ce796b4d/sseclient.py?at=default&fileviewer=file-view-default
"""
# currently excluded from documentation - see docs/README.md

import re
import time

import urllib3

from ldclient.config import HTTPConfig
from ldclient.impl.http import HTTPFactory
from ldclient.util import log
from ldclient.util import throw_if_unsuccessful_response

# Technically, we should support streams that mix line endings.  This regex,
# however, assumes that a system will provide consistent line endings.
end_of_field = re.compile(r'\r\n\r\n|\r\r|\n\n')


class SSEClient:
    def __init__(self, url, last_id=None, retry=3000, connect_timeout=10, read_timeout=300, chunk_size=10000,
                 verify_ssl=False, http=None, http_proxy=None, http_factory=None, **kwargs):
        self.url = url
        self.last_id = last_id
        self.retry = retry
        self._chunk_size = chunk_size

        if http_factory:
            self._timeout = http_factory.timeout
            base_headers = http_factory.base_headers
        else:
            # for backward compatibility in case anyone else is using this class
            self._timeout = urllib3.Timeout(connect=connect_timeout, read=read_timeout)
            base_headers = {}

        # Optional support for passing in an HTTP client
        if http:
            self.http = http
        else:
            hf = http_factory
            if hf is None: # build from individual parameters which we're only retaining for backward compatibility
                hc = HTTPConfig(
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    disable_ssl_verification=not verify_ssl,
                    http_proxy=http_proxy
                )
                hf = HTTPFactory({}, hc)
            self.http = hf.create_pool_manager(1, url)

        # Any extra kwargs will be fed into the request call later.
        self.requests_kwargs = kwargs

        # The SSE spec requires making requests with Cache-Control: nocache
        if 'headers' not in self.requests_kwargs:
            self.requests_kwargs['headers'] = {}

        self.requests_kwargs['headers'].update(base_headers)

        self.requests_kwargs['headers']['Cache-Control'] = 'no-cache'

        # The 'Accept' header is not required, but explicit > implicit
        self.requests_kwargs['headers']['Accept'] = 'text/event-stream'

        # Keep data here as it streams in
        self.buf = u''

        self._connect()

    def _connect(self):
        if self.last_id:
            self.requests_kwargs['headers']['Last-Event-ID'] = self.last_id

        # Use session if set.  Otherwise fall back to requests module.
        self.resp = self.http.request(
            'GET',
            self.url,
            timeout=self._timeout,
            preload_content=False,
            retries=0, # caller is responsible for implementing appropriate retry semantics, e.g. backoff
            **self.requests_kwargs)

        # Raw readlines doesn't work because we may be missing newline characters until the next chunk
        # For some reason, we also need to specify a chunk size because stream=True doesn't seem to guarantee
        # that we get the newlines in a timeline manner
        self.resp_file = self.resp.stream(amt=self._chunk_size)

        # TODO: Ensure we're handling redirects.  Might also stick the 'origin'
        # attribute on Events like the Javascript spec requires.
        throw_if_unsuccessful_response(self.resp)

    def _event_complete(self):
        return re.search(end_of_field, self.buf[len(self.buf)-self._chunk_size-10:]) is not None  # Just search the last chunk plus a bit

    def __iter__(self):
        return self

    def __next__(self):
        while not self._event_complete():
            try:
                nextline = next(self.resp_file)
                # There are some bad cases where we don't always get a line: https://github.com/requests/requests/pull/2431
                if not nextline:
                    raise EOFError()
                self.buf += nextline.decode("utf-8")
            except (StopIteration, EOFError) as e:
                if self.retry:
                    # This retry logic is not what we want in the SDK. It's retained here for backward compatibility in case
                    # anyone else is using SSEClient.
                    time.sleep(self.retry / 1000.0)
                    self._connect()

                    # The SSE spec only supports resuming from a whole message, so
                    # if we have half a message we should throw it out.
                    head, sep, tail = self.buf.rpartition('\n')
                    self.buf = head + sep
                    continue
                else:
                    raise

        split = re.split(end_of_field, self.buf)
        head = split[0]
        tail = "".join(split[1:])

        self.buf = tail
        msg = Event.parse(head)

        # If the server requests a specific retry delay, we need to honor it.
        if msg.retry:
            self.retry = msg.retry

        # last_id should only be set if included in the message.  It's not
        # forgotten if a message omits it.
        if msg.id:
            self.last_id = msg.id

        return msg


class Event:

    sse_line_pattern = re.compile('(?P<name>[^:]*):?( ?(?P<value>.*))?')

    def __init__(self, data='', event='message', id=None, retry=None):
        self.data = data
        self.event = event
        self.id = id
        self.retry = retry

    def dump(self):
        lines = []
        if self.id:
            lines.append('id: %s' % self.id)

        # Only include an event line if it's not the default already.
        if self.event != 'message':
            lines.append('event: %s' % self.event)

        if self.retry:
            lines.append('retry: %s' % self.retry)

        lines.extend('data: %s' % d for d in self.data.split('\n'))
        return '\n'.join(lines) + '\n\n'

    @classmethod
    def parse(cls, raw):
        """
        Given a possibly-multiline string representing an SSE message, parse it
        and return a Event object.
        """
        msg = cls()
        for line in raw.split('\n'):
            m = cls.sse_line_pattern.match(line)
            if m is None:
                # Malformed line.  Discard but warn.
                log.warning('Invalid SSE line: "%s"' % line)
                continue

            name = m.groupdict()['name']
            value = m.groupdict()['value']
            if name == '':
                # line began with a ":", so is a comment.  Ignore
                continue

            if name == 'data':
                # If we already have some data, then join to it with a newline.
                # Else this is it.
                if msg.data:
                    msg.data = '%s\n%s' % (msg.data, value)
                else:
                    msg.data = value
            elif name == 'event':
                msg.event = value
            elif name == 'id':
                msg.id = value
            elif name == 'retry':
                msg.retry = int(value)

        return msg

    def __str__(self):
        return self.data
