from abc import ABCMeta, abstractmethod, abstractproperty
from dataclasses import dataclass
from typing import Any

from ldclient.context import Context
from ldclient.evaluation import EvaluationDetail


@dataclass
class EvaluationSeriesContext:
    """
    Contextual information that will be provided to handlers during evaluation
    series.
    """

    key: str  #: The flag key used to trigger the evaluation.
    context: Context  #: The context used during evaluation.
    default_value: Any  #: The default value provided to the evaluation method
    method: str  #: The string version of the method which triggered the evaluation series.


@dataclass
class Metadata:
    """
    Metadata data class used for annotating hook implementations.
    """

    name: str  #: A name representing a hook instance.


class Hook:
    """
    Abstract class for extending SDK functionality via hooks.

    All provided hook implementations **MUST** inherit from this class.

    This class includes default implementations for all hook handlers. This
    allows LaunchDarkly to expand the list of hook handlers without breaking
    customer integrations.
    """

    __metaclass__ = ABCMeta

    @abstractproperty
    def metadata(self) -> Metadata:
        """
        Get metadata about the hook implementation.
        """
        return Metadata(name='UNDEFINED')

    @abstractmethod
    def before_evaluation(self, series_context: EvaluationSeriesContext, data: dict) -> dict:
        """
        The before method is called during the execution of a variation method
        before the flag value has been determined. The method is executed
        synchronously.

        :param series_context: Contains information about the evaluation being performed. This is not mutable.
        :param data: A record associated with each stage of hook invocations.
            Each stage is called with the data of the previous stage for a series.
            The input record should not be modified.
        :return: Data to use when executing the next state of the hook in the evaluation series.
        """
        return data

    @abstractmethod
    def after_evaluation(self, series_context: EvaluationSeriesContext, data: dict, detail: EvaluationDetail) -> dict:
        """
        The after method is called during the execution of the variation method
        after the flag value has been determined. The method is executed
        synchronously.

        :param series_context: Contains read-only information about the
            evaluation being performed.
        :param data: A record associated with each stage of hook invocations.
            Each stage is called with the data of the previous stage for a series.
        :param detail: The result of the evaluation. This value should not be modified.
        :return: Data to use when executing the next state of the hook in the evaluation series.
        """
        return data


@dataclass
class _EvaluationWithHookResult:
    evaluation_detail: EvaluationDetail
    results: Any = None
