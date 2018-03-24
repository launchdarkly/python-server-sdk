from collections import namedtuple
import pylru


EventSummary = namedtuple('EventSummary', ['start_date', 'end_date', 'counters'])


class EventSummarizer(object):
    def __init__(self, config):
        self.user_keys = pylru.lrucache(config.user_keys_capacity)
        self.start_date = 0
        self.end_date = 0
        self.counters = dict()

    """
    Add to the set of users we've noticed, and return true if the user was already known to us.
    """
    def notice_user(self, user):
        if user is None or 'key' not in user:
            return False
        key = user['key']
        if key in self.user_keys:
            self.user_keys[key]  # refresh cache item
            return True
        self.user_keys[key] = True
        return False

    """
    Reset the set of users we've seen.
    """
    def reset_users(self):
        self.user_keys.clear()

    """
    Add this event to our counters, if it is a type of event we need to count.
    """
    def summarize_event(self, event):
        if event['kind'] == 'feature':
            counter_key = (event['key'], event['variation'], event['version'])
            counter_val = self.counters.get(counter_key)
            if counter_val is None:
                counter_val = { 'count': 1, 'value': event['value'], 'default': event['default'] }
                self.counters[counter_key] = counter_val
            else:
                counter_val['count'] = counter_val['count'] + 1
            date = event['creationDate']
            if self.start_date == 0 or date < self.start_date:
                self.start_date = date
            if date > self.end_date:
                self.end_date = date

    """
    Return a snapshot of the current summarized event data, and reset this state.
    """
    def snapshot(self):
        ret = EventSummary(start_date = self.start_date, end_date = self.end_date, counters = self.counters)
        self.start_date = 0
        self.end_date = 0
        self.counters = dict()
        return ret
