
# This module exists only for historical reasons. Previously, ldclient.flag contained a
# combination of public API types (EvaluationDetail) and implementation details (the evaluate()
# function, etc.). Our new convention is to keep all such implementation details within
# ldclient.impl and its submodules, to make it clear that applications should never try to
# reference them directly. Since some application code may have done so in the past, and since
# we do not want to move anything in the public API yet, we are retaining this module as a
# deprecated entry point and re-exporting some symbols.
#
# In the future, ldclient.evaluation will be the preferred entry point for the public types and
# ldclient.flag will be removed.

from ldclient.evaluation import BigSegmentsStatus, EvaluationDetail
from ldclient.impl.evaluator import Evaluator, EvalResult, error_reason
from ldclient.versioned_data_kind import FEATURES, SEGMENTS

# Deprecated internal function for evaluating flags.
def evaluate(flag, user, store, event_factory) -> EvalResult:
    evaluator = Evaluator(
        lambda key: store.get(FEATURES, key),
        lambda key: store.get(SEGMENTS, key),
        lambda key: (None, BigSegmentsStatus.NOT_CONFIGURED)
    )
    return evaluator.evaluate(flag, user, event_factory)


__all__ = ['EvaluationDetail', 'evaluate', 'error_reason', 'EvalResult']
