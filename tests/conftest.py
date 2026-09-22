import os
import tempfile


_TMP_DB = os.path.join(tempfile.mkdtemp(prefix="locklab-test-"), "test.sqlite3")
os.environ["LOCKLAB_DB"] = _TMP_DB
