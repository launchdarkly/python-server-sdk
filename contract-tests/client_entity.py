import json
import logging
import os
import sys
from typing import Optional

from big_segment_store_fixture import BigSegmentStoreFixture

from ldclient.config import BigSegmentsConfig

# Import ldclient from parent directory
sys.path.insert(1, os.path.join(sys.path[0], '..'))
from ldclient import *


class ClientEntity:
    def __init__(self, tag, config):
        self.log = logging.getLogger(tag)
        opts = {"sdk_key": config["credential"]}

        if config.get("streaming") is not None:
            streaming = config["streaming"]
            if streaming.get("baseUri") is not None:
                opts["stream_uri"] = streaming["baseUri"]
            _set_optional_time_prop(streaming, "initialRetryDelayMs", opts, "initial_reconnect_delay")

        if config.get("events") is not None:
            events = config["events"]
            if events.get("baseUri") is not None:
                opts["events_uri"] = events["baseUri"]
            if events.get("capacity") is not None:
                opts["events_max_pending"] = events["capacity"]
            opts["diagnostic_opt_out"] = not events.get("enableDiagnostics", False)
            opts["all_attributes_private"] = events.get("allAttributesPrivate", False)
            opts["private_attribute_names"] = events.get("globalPrivateAttributes", {})
            _set_optional_time_prop(events, "flushIntervalMs", opts, "flush_interval")
        else:
            opts["send_events"] = False

        if config.get("bigSegments") is not None:
            big_params = config["bigSegments"]
            big_config = {
                "store": BigSegmentStoreFixture(big_params["callbackUri"])
            }
            if big_params.get("userCacheSize") is not None:
                big_config["context_cache_size"] = big_params["userCacheSize"]
            _set_optional_time_prop(big_params, "userCacheTimeMs", big_config, "context_cache_time")
            _set_optional_time_prop(big_params, "statusPollIntervalMs", big_config, "status_poll_interval")
            _set_optional_time_prop(big_params, "staleAfterMs", big_config, "stale_after")
            opts["big_segments"] = BigSegmentsConfig(**big_config)
        
        start_wait = config.get("startWaitTimeMs") or 5000
        config = Config(**opts)

        self.client = client.LDClient(config, start_wait / 1000.0)

    def is_initializing(self) -> bool:
        return self.client.is_initialized()

    def evaluate(self, params: dict) -> dict:
        response = {}

        if params.get("detail", False):
            detail = self.client.variation_detail(params["flagKey"], params["context"], params["defaultValue"])
            response["value"] = detail.value
            response["variationIndex"] = detail.variation_index
            response["reason"] = detail.reason
        else:
            response["value"] = self.client.variation(params["flagKey"], params["context"], params["defaultValue"])

        return response

    def evaluate_all(self, params: dict):
        opts = {}
        opts["client_side_only"] = params.get("clientSideOnly", False)
        opts["with_reasons"] = params.get("withReasons", False)
        opts["details_only_for_tracked_flags"] = params.get("detailsOnlyForTrackedFlags", False)

        state = self.client.all_flags_state(params["context"], **opts)

        return {"state": state.to_json_dict()}

    def track(self, params: dict):
        self.client.track(params["eventKey"], params["context"], params["data"], params.get("metricValue", None))

    def identify(self, params: dict):
        self.client.identify(params["context"])

    def flush(self):
        self.client.flush()

    def secure_mode_hash(self, params: dict) -> dict:
        return {"result": self.client.secure_mode_hash(params["context"])}
    
    def context_build(self, params: dict) -> dict:
        if params.get("multi"):
            b = Context.multi_builder()
            for c in params.get("multi"):
                b.add(self._context_build_single(c))
            return self._context_response(b.build())
        return self._context_response(self._context_build_single(params["single"]))
    
    def _context_build_single(self, params: dict) -> Context:
        b = Context.builder(params["key"])
        if "kind" in params:
            b.kind(params["kind"])
        if "name" in params:
            b.name(params["name"])
        if "anonymous" in params:
            b.anonymous(params["anonymous"])
        if "custom" in params:
            for k, v in params.get("custom").items():
                b.set(k, v)
        if "private" in params:
            for attr in params.get("private"):
                b.private(attr)
        return b.build()
    
    def context_convert(self, params: dict) -> dict:
        input = params["input"]
        try:
            props = json.loads(input)
            return self._context_response(Context.from_dict(props))
        except Exception as e:
            return {"error": str(e)}
    
    def _context_response(self, c: Context) -> dict:
        if c.valid:
            return {"output": c.to_json_string()}
        return {"error": c.error}
    
    def get_big_segment_store_status(self) -> dict:
        status = self.client.big_segment_store_status_provider.status
        return {
            "available": status.available,
            "stale": status.stale
        }

    def close(self):
        self.client.close()
        self.log.info('Test ended')

def _set_optional_time_prop(params_in: dict, name_in: str, params_out: dict, name_out: str):
    if params_in.get(name_in) is not None:
        params_out[name_out] = params_in[name_in] / 1000.0
    return None
