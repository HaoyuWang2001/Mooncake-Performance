"""pytest bootstrap: keep loopback traffic out of any HTTP proxy.

The test suite starts real local servers (WebUI backend, mock etcd gateway)
on 127.0.0.1:<random port> and talks to them through urllib. Python's urllib
only honours literal host entries in no_proxy (no "127.*" wildcards), so on
machines whose proxy environment uses that syntax (common in WSL) every
loopback request would be sent to the proxy and fail with 502 / empty
bodies. Prepending the loopback names here makes the suite independent of
the surrounding shell/IDE environment.
"""
import os

_LOOPBACK = ("127.0.0.1", "localhost")
for _key in ("no_proxy", "NO_PROXY"):
    _entries = [e for e in os.environ.get(_key, "").split(",") if e]
    _missing = [h for h in _LOOPBACK if h not in _entries]
    if _missing:
        os.environ[_key] = ",".join(_missing + _entries)
