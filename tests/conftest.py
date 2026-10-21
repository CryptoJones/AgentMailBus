"""The permission tests build files whose modes matter; pin the umask so a host's
setting cannot widen or narrow them."""

import os

import pytest


@pytest.fixture(autouse=True, scope="session")
def _umask_022():
    old = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(old)
