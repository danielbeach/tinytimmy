import inspect
import yaml
from tinytimmy.data_quality import DataQuality
from dataclasses import dataclass


@dataclass
class Test:
    id: int
    name: str


class ConfigParser:
    def __init__(self, path: str):
        available_tests = self.get_available_tests()
        parsed_conf = self.read_yaml(path)
        self.test_objects = []
        self.prepare_testconf(parsed_conf, available_tests)

    @staticmethod
    def read_yaml(path: str) -> dict:
        with open(path, "r") as conf_file:
            return yaml.safe_load(conf_file)

    @staticmethod
    def get_available_tests() -> list:
        class_metadata = inspect.getmembers(DataQuality, predicate=inspect.isfunction)
        available_methods = [
            metadata[0]
            for metadata in class_metadata
            if not metadata[0].startswith("_")
        ]
        return available_methods

    def prepare_testconf(self, parsed_conf: dict, available_tests: list) -> list:
        if not parsed_conf:
            raise TypeError("Toml conf cannot be empty")
        tests = parsed_conf.get("Tests")
        for test in tests:
            if test.get("name") in available_tests:
                self.test_objects.append(Test(**test))
            else:
                raise ValueError("Non existent test")
        return self.test_objects

    @property
    def test_objects(self):
        return self._test_objects

    @test_objects.setter
    def test_objects(self, value):
        if value is None:
            raise ValueError("Cannot be empty")
        self._test_objects = value
