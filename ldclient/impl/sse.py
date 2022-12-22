from ldclient.config import HTTPConfig
from ldclient.impl.http import HTTPFactory
from ldclient.impl.util import throw_if_unsuccessful_response


class _BufferedLineReader:
    """
    Helper class that encapsulates the logic for reading UTF-8 stream data as a series of text lines,
    each of which can be terminated by \n, \r, or \r\n.
    """
    def lines_from(chunks):
        """
        Takes an iterable series of encoded chunks (each of "bytes" type) and parses it into an iterable
        series of strings, each of which is one line of text. The line does not include the terminator.
        """
        last_char_was_cr = False
        partial_line = None

        for chunk in chunks:
            if len(chunk) == 0:
                continue

            # bytes.splitlines() will correctly break lines at \n, \r, or \r\n, and is faster than
            # iterating through the characters in Python code. However, we have to adjust the results
            # in several ways as described below.
            lines = chunk.splitlines()
            if last_char_was_cr:
                last_char_was_cr = False
                if chunk[0] == 10:
                    # If the last character we saw was \r, and then the first character in buf is \n, then
                    # that's just a single \r\n terminator, so we should remove the extra blank line that
                    # splitlines added for that first \n.
                    lines.pop(0)
                    if len(lines) == 0:
                        continue  # ran out of data, continue to get next chunk
            if partial_line is not None:
                # On our last time through the loop, we ended up with an unterminated line, so we should
                # treat our first parsed line here as a continuation of that.
                lines[0] = partial_line + lines[0]
                partial_line = None
            # Check whether the buffer really ended in a terminator. If it did not, then the last line in
            # lines is a partial line and should not be emitted yet.
            last_char = chunk[len(chunk)-1]
            if last_char == 13:
                last_char_was_cr = True  # remember this in case the next chunk starts with \n
            elif last_char != 10:
                partial_line = lines.pop()  # remove last element which is the partial line
            for line in lines:
                yield line.decode()
            

class Event:
    """
    An event received by SSEClient.
    """
    def __init__(self, event='message', data='', last_event_id=None):
        self._event = event
        self._data = data
        self._id = last_event_id

    @property
    def event(self):
        """
        The event type, or "message" if not specified.
        """
        return self._event

    @property
    def data(self):
        """
        The event data.
        """
        return self._data

    @property
    def last_event_id(self):
        """
        The last non-empty "id" value received from this stream so far.
        """
        return self._id

    def dump(self):
        lines = []
        if self.id:
            lines.append('id: %s' % self.id)

        # Only include an event line if it's not the default already.
        if self.event != 'message':
            lines.append('event: %s' % self.event)

        lines.extend('data: %s' % d for d in self.data.split('\n'))
        return '\n'.join(lines) + '\n\n'


class SSEClient:
    """
    A simple Server-Sent Events client.

    This implementation does not include automatic retrying of a dropped connection; the caller will do that.
    If a connection ends, the events iterator will simply end.
    """
    def __init__(self, url, last_id=None, http_factory=None, **kwargs):
        self.url = url
        self.last_id = last_id
        self._chunk_size = 10000
        
        if http_factory is None:
            http_factory = HTTPFactory({}, HTTPConfig())
        self._timeout = http_factory.timeout
        base_headers = http_factory.base_headers

        self.http = http_factory.create_pool_manager(1, url)

        # Any extra kwargs will be fed into the request call later.
        self.requests_kwargs = kwargs

        # The SSE spec requires making requests with Cache-Control: nocache
        if 'headers' not in self.requests_kwargs:
            self.requests_kwargs['headers'] = {}

        self.requests_kwargs['headers'].update(base_headers)

        self.requests_kwargs['headers']['Cache-Control'] = 'no-cache'

        # The 'Accept' header is not required, but explicit > implicit
        self.requests_kwargs['headers']['Accept'] = 'text/event-stream'

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

    @property
    def events(self):
        """
        An iterable series of Event objects received from the stream.
        """
        event_type = ""
        event_data = None
        for line in _BufferedLineReader.lines_from(self.resp_file):
            if line == "":
                if event_data is not None:
                    yield Event("message" if event_type == "" else event_type, event_data, self.last_id)
                event_type = ""
                event_data = None
                continue
            colon_pos = line.find(':')
            if colon_pos < 0:
                continue  # malformed line - ignore
            if colon_pos == 0:
                continue  # comment - currently we're not surfacing these
            name = line[0:colon_pos]
            if colon_pos < (len(line) - 1) and line[colon_pos + 1] == ' ':
                colon_pos += 1
            value = line[colon_pos+1:]
            if name == 'event':
                event_type = value
            elif name == 'data':
                event_data = value if event_data is None else (event_data + "\n" + value)
            elif name == 'id':
                self.last_id = value
            elif name == 'retry':
                pass  # auto-reconnect is not implemented in this simplified client
            # unknown field names are ignored in SSE

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
