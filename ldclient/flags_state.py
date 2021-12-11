
# This module exists only for historical reasons. It only contained the FeatureFlagsState class,
# which is now in the ldclient.evaluation module. We are retaining this module as a deprecated
# entry point and re-exporting the class from ldclient.evaluation.
#
# In the future, ldclient.evaluation will be the preferred entry point and ldclient.flags_state
# will be removed.
from ldclient.evaluation import FeatureFlagsState
