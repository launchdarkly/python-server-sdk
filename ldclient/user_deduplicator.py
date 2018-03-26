import pylru


class UserDeduplicator(object):
    def __init__(self, config):
        self.user_keys = pylru.lrucache(config.user_keys_capacity)

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
