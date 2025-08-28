from abc import abstractmethod
import json
from typing import Any
import pytest
from common_scenario import CommonScenario
from testing_utils import ScenarioResult, LogContainer, BazelTools, BuildTools


class TestSupportedDatatypesKeys(CommonScenario):
    @pytest.fixture(scope="class")
    def scenario_name(self) -> str:
        return "cit.supported_datatypes.keys"

    @pytest.fixture(scope="class")
    def test_config(self) -> dict[str, Any]:
        return {"kvs_parameters": {"instance_id": 1, "flush_on_exit": False}}

    @pytest.fixture(scope="class", params=["rust"])
    def build_tools(self, request: pytest.FixtureRequest) -> BuildTools:
        version = request.param
        return BazelTools(option_prefix=version)

    def test_ok(self, results: ScenarioResult, logs_info_level: LogContainer) -> None:
        assert results.return_code == 0

        logs = logs_info_level.get_logs_by_field(field="key", pattern=".*").get_logs()
        act_keys = set(map(lambda x: x.key, logs))
        exp_keys = {"example", "emoji ✅❗😀", "greek ημα"}

        assert len(act_keys) == len(exp_keys)
        assert len(act_keys.symmetric_difference(exp_keys)) == 0


class TestSupportedDatatypesValues(CommonScenario):
    @abstractmethod
    def exp_key(self) -> str:
        pass

    @abstractmethod
    def exp_value(self) -> Any:
        pass

    def exp_tagged(self) -> dict[str, Any]:
        return {"t": self.exp_key(), "v": self.exp_value()}

    @pytest.fixture(scope="class")
    def scenario_name(self) -> str:
        return f"cit.supported_datatypes.values.{self.exp_key()}"

    @pytest.fixture(scope="class")
    def test_config(self) -> dict[str, Any]:
        return {"kvs_parameters": {"instance_id": 1, "flush_on_exit": False}}

    @pytest.fixture(scope="class", params=["rust"])
    def build_tools(self, request: pytest.FixtureRequest) -> BuildTools:
        version = request.param
        return BazelTools(option_prefix=version)

    def test_ok(self, results: ScenarioResult, logs_info_level: LogContainer) -> None:
        assert results.return_code == 0

        # Get log containing type and value.
        logs = logs_info_level.get_logs_by_field(
            field="key", value=self.exp_key()
        ).get_logs()
        assert len(logs) == 1
        log = logs[0]

        # Assert key.
        act_key = log.key
        assert act_key == self.exp_key()

        # Assert values.
        act_value = json.loads(log.value)
        assert act_value == self.exp_tagged()


class TestSupportedDatatypesValues_I32(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "i32"

    def exp_value(self) -> Any:
        return -321


class TestSupportedDatatypesValues_U32(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "u32"

    def exp_value(self) -> Any:
        return 1234


class TestSupportedDatatypesValues_I64(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "i64"

    def exp_value(self) -> Any:
        return -123456789


class TestSupportedDatatypesValues_U64(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "u64"

    def exp_value(self) -> Any:
        return 123456789


class TestSupportedDatatypesValues_F64(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "f64"

    def exp_value(self) -> Any:
        return -5432.1


class TestSupportedDatatypesValues_Bool(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "bool"

    def exp_value(self) -> Any:
        return True


class TestSupportedDatatypesValues_String(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "str"

    def exp_value(self) -> Any:
        return "example"


class TestSupportedDatatypesValues_Array(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "arr"

    def exp_value(self) -> Any:
        return [
            {"t": "f64", "v": 321.5},
            {"t": "bool", "v": False},
            {"t": "str", "v": "hello"},
            {"t": "null", "v": None},
            {"t": "arr", "v": []},
            {
                "t": "obj",
                "v": {
                    "sub-number": {
                        "t": "f64",
                        "v": 789,
                    },
                },
            },
        ]


class TestSupportedDatatypesValues_Object(TestSupportedDatatypesValues):
    def exp_key(self) -> str:
        return "obj"

    def exp_value(self) -> Any:
        return {"sub-number": {"t": "f64", "v": 789}}
