#!/usr/bin/env python3
"""Guard: loopback hosts must bypass any HTTP proxy inside the test process.

conftest.py prepends 127.0.0.1/localhost to no_proxy because urllib does not
understand wildcard entries like "127.*" (a common WSL proxy setup). Without
that, every local test server (WebUI backend, mock etcd gateway) is reached
through the proxy and answers 502 / an empty body.
"""
import urllib.request


def test_loopback_bypasses_proxy():
    assert urllib.request.proxy_bypass("127.0.0.1")
    assert urllib.request.proxy_bypass("localhost")
