import pytest

from tinytimmy.config_parser import ConfigParser, Test


POSITIVE_YAML = "./tests/test_yamls/positive_test.yaml"
NEGATIVE_YAML = "./tests/test_yamls/negative_test.yaml"
EMPTY_YAML = "./tests/test_yamls/empty.yaml"


def test_objects_structure():
    conf = ConfigParser(POSITIVE_YAML)
    assert conf.test_objects == [Test(id=1, name="null_check")]


def test_non_existent_test():
    with pytest.raises(ValueError, match="Non existent test"):
        ConfigParser(NEGATIVE_YAML)


def test_empty_yaml():
    with pytest.raises(TypeError, match="Toml conf cannot be empty"):
        ConfigParser(EMPTY_YAML)
