"""The permission tests build files and directories whose modes matter. Pin the
umask so a host's setting cannot widen or narrow them, and give every directory
its search bit back afterwards so pytest can remove the temporary tree."""

import os
import stat

import pytest


@pytest.fixture(autouse=True, scope="session")
def _umask_022():
    old = os.umask(0o022)
    try:
        yield
    finally:
        os.umask(old)


@pytest.fixture(autouse=True)
def _removable_tmp(tmp_path):
    yield
    for dirpath, dirnames, _ in os.walk(tmp_path):
        for name in dirnames:
            child = os.path.join(dirpath, name)
            try:
                if not os.path.islink(child):
                    os.chmod(child, stat.S_IMODE(os.lstat(child).st_mode) | 0o700)
            except OSError:
                pass
