import dataclasses
import json
from dataclasses import dataclass
from enum import Enum
from typing import List, Dict

import dataconf
from pyhocon import ConfigTree


class ConfigurationBase:
    @staticmethod
    def _convert_private_value(value: str):
        return value.replace('"#', '"pswd_')

    @staticmethod
    def _convert_private_value_inv(value: str):
        if value and value.startswith("pswd_"):
            return value.replace("pswd_", "#", 1)
        else:
            return value

    @classmethod
    def load_from_dict(cls, configuration: dict):
        """
        Initialize the configuration dataclass object from dictionary.
        Args:
            configuration: Dictionary loaded from json configuration.

        Returns:

        """
        json_conf = json.dumps(configuration)
        json_conf = ConfigurationBase._convert_private_value(json_conf)
        return dataconf.loads(json_conf, cls, ignore_unexpected=True)

    @classmethod
    def get_dataclass_required_parameters(cls) -> List[str]:
        """
        Return list of required parameters based on the dataclass definition (no default value)
        Returns: List[str]

        """
        return [cls._convert_private_value_inv(f.name)
                for f in dataclasses.fields(cls)
                if f.default == dataclasses.MISSING
                and f.default_factory == dataclasses.MISSING]


class Endpoint(str, Enum):
    USER = "user"
    CONTENT = "content",
    EVENT = "event",
    RETURN = "return"


class ApiMethod(str, Enum):
    POST = "POST"
    DELETE = "DELETE"


class DataType(Enum):
    bool = 'bool'
    string = 'string'
    number = 'number'
    object = 'object'


class LoadMode(str, Enum):
    endpoint = 'endpoint'
    users_bulk = 'users_bulk'


@dataclass
class ColumnDataTypes(ConfigurationBase):
    autodetect: bool = False
    datatype_override: List[Dict[str, str]] = dataclasses.field(default_factory=list)


@dataclass
class Destination(ConfigurationBase):
    mode: LoadMode = LoadMode.users_bulk
    endpoint: Endpoint = Endpoint.USER
    method: ApiMethod = ApiMethod.POST


@dataclass
class JsonMapping():
    nesting_delimiter: str = '__'
    column_data_types: ColumnDataTypes = dataclasses.field(default_factory=lambda: ConfigTree({}))
    # column_names_override: Optional[dict] = dataclasses.field(default_factory=dict)


@dataclass
class Configuration(ConfigurationBase):
    pswd_api_key: str
    pswd_secret: str
    destination: Destination = dataclasses.field(default_factory=lambda: Destination())
    json_mapping: JsonMapping = dataclasses.field(default_factory=lambda: JsonMapping())
