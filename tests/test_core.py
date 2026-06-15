import pytest

from rmq_message_pruner.cli import parse_args, should_drop


def test_should_drop_any_case_sensitive():
    assert should_drop("alpha beta", ["beta"], "any", False) is True
    assert should_drop("alpha beta", ["gamma"], "any", False) is False


def test_should_drop_all():
    assert should_drop("alpha beta", ["alpha", "beta"], "all", False) is True
    assert should_drop("alpha beta", ["alpha", "gamma"], "all", False) is False


def test_should_drop_ignore_case():
    assert should_drop("Alpha", ["alpha"], "any", True) is True


def test_republish_rejects_parallel_workers():
    with pytest.raises(SystemExit):
        parse_args(["--queue", "jobs", "--republish", "--workers", "2"])
