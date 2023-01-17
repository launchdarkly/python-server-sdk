import logging
import os
import sys

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from ldclient import *


class ClientEntity:
    def __init__(self, tag, config):
        self.log = logging.getLogger(tag)
        opts = {"sdk_key": config["credential"]}

        tags = config.get('tags', {})
        if tags:
            opts['application'] = {
                'id': tags.get('applicationId', ''),
                'version': tags.get('applicationVersion', ''),
            }

        if config.get("streaming") is not None:
            streaming = config["streaming"]
            if streaming.get("baseUri") is not None:
                opts["stream_uri"] = streaming["baseUri"]
            if streaming.get("initialRetryDelayMs") is not None:
                opts["initial_reconnect_delay"] = streaming["initialRetryDelayMs"] / 1000.0

        if config.get("events") is not None:
            events = config["events"]
            if events.get("baseUri") is not None:
                opts["events_uri"] = events["baseUri"]
            if events.get("capacity") is not None:
                opts["events_max_pending"] = events["capacity"]
            opts["diagnostic_opt_out"] = not events.get("enableDiagnostics", False)
            opts["all_attributes_private"] = events.get("allAttributesPrivate", False)
            opts["private_attribute_names"] = events.get("globalPrivateAttributes", {})
            if events.get("flushIntervalMs") is not None:
                 opts["flush_interval"] = events["flushIntervalMs"] / 1000.0
            if events.get("inlineUsers") is not None:
                opts["inline_users_in_events"] = events["inlineUsers"]
        else:
            opts["send_events"] = False

        start_wait = config.get("startWaitTimeMs") or 5000
        config = Config(**opts)

        self.client = client.LDClient(config, start_wait / 1000.0)

    def is_initializing(self) -> bool:
        return self.client.is_initialized()

    def evaluate(self, params) -> dict:
        response = {}

        if params.get("detail", False):
            detail = self.client.variation_detail(params["flagKey"], params["user"], params["defaultValue"])
            response["value"] = detail.value
            response["variationIndex"] = detail.variation_index
            response["reason"] = detail.reason
        else:
            response["value"] = self.client.variation(params["flagKey"], params["user"], params["defaultValue"])

        return response

    def evaluate_all(self, params):
        opts = {}
        opts["client_side_only"] = params.get("clientSideOnly", False)
        opts["with_reasons"] = params.get("withReasons", False)
        opts["details_only_for_tracked_flags"] = params.get("detailsOnlyForTrackedFlags", False)

        state = self.client.all_flags_state(params["user"], **opts)

        return {"state": state.to_json_dict()}

    def track(self, params):
        self.client.track(params["eventKey"], params["user"], params["data"], params.get("metricValue", None))

    def identify(self, params):
        self.client.identify(params["user"])

    def alias(self, params):
        self.client.alias(params["user"], params["previousUser"])

    def flush(self):
        self.client.flush()

    def close(self):
        self.client.close()
        self.log.info('Test ended')
