import jsonpickle
import six


class EventSerializer:
    IGNORE_ATTRS = frozenset(['key', 'custom', 'anonymous'])
    ALLOWED_TOP_LEVEL_ATTRS = frozenset(['key', 'secondary', 'ip', 'country', 'email',
        'firstName', 'lastName', 'avatar', 'name', 'anonymous', 'custom'])

    def __init__(self, config):
        self._private_attribute_names = config.private_attribute_names
        self._all_attributes_private = config.all_attributes_private
        
    def serialize_events(self, events):
        body = [events] if isinstance(events, dict) else events
        filtered = [ self._filter_event(e) for e in body ]
        return jsonpickle.encode(filtered, unpicklable=False)

    def _is_private_attr(self, name, user_private_attrs):
        if name in EventSerializer.IGNORE_ATTRS:
            return False
        elif self._all_attributes_private:
            return True
        else:
            return (name in self._private_attribute_names) or (name in user_private_attrs)

    def _filter_event(self, e):
        def filter_user_props(user_props):
            all_private_attrs = set()
            user_private_attrs = user_props.get('privateAttributeNames', [])

            def filter_private_attrs(attrs, allowed_attrs = frozenset()):
                for key, value in six.iteritems(attrs):
                    if (not allowed_attrs) or (key in allowed_attrs):
                        if self._is_private_attr(key, user_private_attrs):
                            all_private_attrs.add(key)
                        else:
                            yield key, value

            ret = dict(filter_private_attrs(user_props, EventSerializer.ALLOWED_TOP_LEVEL_ATTRS))
            if 'custom' in user_props:
                ret['custom'] = dict(filter_private_attrs(user_props['custom']))

            if all_private_attrs:
                ret['privateAttrs'] = sorted(list(all_private_attrs))  # note, only sorting to make tests reliable
            return ret

        return dict((key, filter_user_props(value) if key == 'user' else value) for (key, value) in six.iteritems(e))
