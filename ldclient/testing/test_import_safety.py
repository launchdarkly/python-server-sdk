import subprocess
import sys


def test_import_ldclient_does_not_require_aiohttp():
    """A bare ``import ldclient`` must not import aiohttp.

    aiohttp is an optional dependency (the ``async`` extra); sync-only installs
    do not have it. The async code keeps its aiohttp-importing pieces (the async
    client and HTTP transport) off the eager import path, so a sync-only user
    can still ``import ldclient``. ``import ldclient`` also loads
    ``ldclient.migrations`` (see ``ldclient/__init__.py``), so this covers the
    async migration surface too.

    This guards the invariant across all async work: no change may make
    ``import ldclient`` pull in aiohttp. A fresh interpreter is used so the
    result is not affected by other tests that already imported aiohttp.
    """
    code = "import ldclient, sys; sys.exit(1 if 'aiohttp' in sys.modules else 0)"
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, (
        "importing ldclient pulled in aiohttp. Keep aiohttp off the eager "
        "import path (import async_client / transport lazily).\n"
        "stderr:\n%s" % result.stderr
    )
