"""Tests for the repository test-layout profiler (dag_healer.repo.profile)."""

from __future__ import annotations

import hashlib
from pathlib import Path

from dag_healer.repo.profile import (
    TestLayout,
    detect_test_layout,
    file_sha256,
    iter_repo_files,
    snapshot_hashes,
)

PYTEST_FUNCTIONS = """
    import pytest

    from src.etl.sales import transform


    def test_transform():
        assert transform() is None


    @pytest.mark.parametrize("value", [1, 2])
    def test_values(value):
        assert value
"""

UNITTEST_CLASSES = """
    import unittest

    from etl.sales import transform


    class TransformTest(unittest.TestCase):
        def test_transform(self):
            self.assertIsNone(transform())
"""


def test_repo_without_tests(make_repo) -> None:
    root = make_repo({"dags/a.py": "x = 1\n", "src/etl/sales.py": "def transform():\n    pass\n"})
    layout = detect_test_layout(root)
    assert layout == TestLayout(tests_dir=None, exists=False, framework=None, has_conftest=False)
    assert layout.file_pattern == "test_*.py"
    assert layout.config_file is None


def test_pytest_tests_dir_with_conftest_and_pyproject(make_repo) -> None:
    root = make_repo(
        {
            "pyproject.toml": '[project]\nname = "x"\n\n[tool.pytest.ini_options]\naddopts = "-q"\n',
            "tests/conftest.py": "import pytest\n",
            "tests/test_sales.py": PYTEST_FUNCTIONS,
            "tests/test_more.py": "from src.etl import sales\n\ndef test_more():\n    assert sales\n",
            "src/etl/sales.py": "def transform():\n    return None\n",
        }
    )
    layout = detect_test_layout(root)
    assert layout.tests_dir == "tests"
    assert layout.exists is True
    assert layout.framework == "pytest"
    assert layout.has_conftest is True
    assert layout.uses_classes is False
    assert layout.config_file == "pyproject.toml"
    assert layout.file_pattern == "test_*.py"
    assert layout.import_style == "from src.etl import sales"


def test_unittest_classes_in_test_dir(make_repo) -> None:
    root = make_repo(
        {
            "test/test_transform.py": UNITTEST_CLASSES,
            "test/test_other.py": "import unittest\n\nclass OtherTest(unittest.TestCase):\n"
            "    def test_x(self):\n        pass\n",
            "src/etl/sales.py": "def transform():\n    return None\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.exists, layout.framework) == ("test", True, "unittest")
    assert layout.uses_classes is True
    assert layout.has_conftest is False
    assert layout.import_style == "from etl.sales import transform"


def test_plain_test_classes_are_pytest_style(make_repo) -> None:
    root = make_repo(
        {
            "tests/test_cls.py": "class TestThing:\n    def test_a(self):\n        assert True\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.framework, layout.uses_classes) == ("pytest", True)
    assert layout.import_style is None


def test_pytest_ini_testpaths_and_python_files(make_repo) -> None:
    root = make_repo(
        {
            "pytest.ini": "[pytest]\ntestpaths = checks\npython_files = check_*.py\n",
            "checks/check_sales.py": "def test_x():\n    pass\n",
            "tests/test_ignored.py": "def test_y():\n    pass\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.config_file, layout.file_pattern) == (
        "checks",
        "pytest.ini",
        "check_*.py",
    )
    assert layout.framework == "pytest"


def test_setup_cfg_and_tox_ini_sections(make_repo) -> None:
    setup_root = make_repo(
        {"setup.cfg": "[metadata]\nname = x\n\n[tool:pytest]\naddopts = -q\n", "tests/test_a.py": "x = 1\n"},
        name="setup_cfg",
    )
    assert detect_test_layout(setup_root).config_file == "setup.cfg"
    tox_root = make_repo(
        {"tox.ini": "[tox]\nenvlist = py\n\n[pytest]\nminversion = 7\n", "tests/test_a.py": "x = 1\n"},
        name="tox_ini",
    )
    assert detect_test_layout(tox_root).config_file == "tox.ini"
    no_section = make_repo({"setup.cfg": "[metadata]\nname = x\n"}, name="no_section")
    layout = detect_test_layout(no_section)
    assert (layout.config_file, layout.framework) == (None, None)


def test_native_pytest_toml_table(make_repo) -> None:
    root = make_repo(
        {
            "pyproject.toml": '[tool.pytest]\ntestpaths = ["qa"]\n',
            "qa/test_q.py": "def test_q():\n    pass\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.config_file) == ("qa", "pyproject.toml")


def test_suffix_pattern_outside_conventional_dirs(make_repo) -> None:
    root = make_repo(
        {
            "src/pkg/sales_test.py": "def test_a():\n    pass\n",
            "src/pkg/customers_test.py": "def test_b():\n    pass\n",
            "src/other/test_lonely.py": "def test_c():\n    pass\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.file_pattern, layout.exists) == ("src/pkg", "*_test.py", True)


def test_src_tests_conventional_dir(make_repo) -> None:
    root = make_repo({"src/tests/test_a.py": "import pytest\n\ndef test_a():\n    pass\n"})
    assert detect_test_layout(root).tests_dir == "src/tests"


def test_pytest_config_without_tests(make_repo) -> None:
    root = make_repo({"pyproject.toml": "[tool.pytest.ini_options]\nminversion = '8'\n"})
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.exists, layout.framework) == (None, False, "pytest")
    assert layout.config_file == "pyproject.toml"


def test_conftest_only_tests_dir_counts_as_existing(make_repo) -> None:
    root = make_repo({"tests/conftest.py": "import pytest\n"})
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.exists, layout.has_conftest, layout.framework) == (
        "tests",
        True,
        True,
        "pytest",
    )


def test_unparsable_test_files_and_ignored_dirs(make_repo) -> None:
    root = make_repo(
        {
            "tests/test_broken.py": "def test_x(:\n",
            "tests/test_ok.py": "def test_ok():\n    pass\n",
            ".venv/lib/test_vendored.py": "def test_v():\n    pass\n",
            "node_modules/pkg/test_js.py": "def test_n():\n    pass\n",
        }
    )
    layout = detect_test_layout(root)
    assert (layout.tests_dir, layout.framework) == ("tests", "pytest")
    walked = {p.relative_to(root).as_posix() for p in iter_repo_files(root, suffixes=(".py",))}
    assert walked == {"tests/test_broken.py", "tests/test_ok.py"}


def test_virtualenv_detected_by_pyvenv_cfg(make_repo) -> None:
    root = make_repo({"env/pyvenv.cfg": "home = x\n", "env/test_x.py": "def test():\n    pass\n"})
    assert detect_test_layout(root).exists is False


def test_test_layout_is_not_collected_and_serialises(make_repo) -> None:
    assert TestLayout.__test__ is False
    layout = detect_test_layout(make_repo({"tests/test_a.py": "def test_a():\n    pass\n"}))
    assert layout.to_dict()["tests_dir"] == "tests"


# --------------------------------------------------------------------------- hashing


def test_file_sha256_matches_hashlib(tmp_path: Path) -> None:
    target = tmp_path / "f.bin"
    target.write_bytes(b"dag-healer" * 10000)
    assert file_sha256(target) == hashlib.sha256(b"dag-healer" * 10000).hexdigest()


def test_snapshot_hashes_patterns_and_skips(make_repo) -> None:
    root = make_repo(
        {
            "dags/a.py": "a = 1\n",
            "dags/sub/b.py": "b = 1\n",
            "dags/readme.md": "doc\n",
            "src/etl/c.py": "c = 1\n",
            "dags/__pycache__/a.cpython-313.py": "junk\n",
            ".git/hooks/x.py": "junk\n",
            ".venv/lib/site.py": "junk\n",
        }
    )
    everything = snapshot_hashes(root)
    assert sorted(everything) == ["dags/a.py", "dags/sub/b.py", "src/etl/c.py"]
    assert everything["dags/a.py"] == file_sha256(root / "dags" / "a.py")
    assert sorted(snapshot_hashes(root / "dags")) == ["a.py", "sub/b.py"]
    assert sorted(snapshot_hashes(root, ["dags/*.py"])) == ["dags/a.py"]
    assert sorted(snapshot_hashes(root, ("dags/**", "*.md"))) == [
        "dags/a.py",
        "dags/readme.md",
        "dags/sub/b.py",
    ]
    assert snapshot_hashes(root / "missing") == {}


def test_snapshot_detects_modifications(make_repo) -> None:
    root = make_repo({"dags/a.py": "a = 1\n"})
    before = snapshot_hashes(root)
    (root / "dags" / "a.py").write_text("a = 2\n", encoding="utf-8")
    after = snapshot_hashes(root)
    assert before.keys() == after.keys() and before != after
