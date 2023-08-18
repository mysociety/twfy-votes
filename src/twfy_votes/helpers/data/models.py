import json
from enum import Enum, EnumMeta, auto
from pathlib import Path
from typing import (
    Any,
    Protocol,
    TypeVar,
    runtime_checkable,
)

from pydantic import BaseModel
from ruamel.yaml import YAML
from typing_extensions import Self


class ModelDumpable(Protocol):
    def model_dump(self) -> dict[str, Any]:
        ...


@runtime_checkable
class SchemaCompatible(Protocol):
    """
    Basically anything that has a model_json_schema method is good.
    Rather than getting into strict issues with inheritance
    """

    @classmethod
    def model_json_schema(cls) -> dict[str, Any]:
        ...

    def model_dump_json(self) -> str:
        ...

    def model_dump(self) -> dict[str, Any]:
        ...


class StashableBaseProtocol(SchemaCompatible, Protocol):
    def to_path(self, path: Path):
        ...

    @classmethod
    def from_path(cls, path: Path) -> Self:
        ...


ResponseObject = TypeVar("ResponseObject", bound=SchemaCompatible)
DumpableType = TypeVar("DumpableType", bound=ModelDumpable)
StashableType = TypeVar("StashableType", bound=StashableBaseProtocol)


def data_to_yaml(data: dict[str, Any], path: Path) -> None:
    yaml = YAML()

    # register all subclasses of StrEnum
    for subclass in StrEnum.__subclasses__():
        yaml.register_class(subclass)

    yaml.dump(data, path)


def yaml_to_data(path: Path) -> dict[str, Any]:
    yaml = YAML(typ="base")

    return yaml.load(path)


def data_to_json(data: dict[str, Any], path: Path) -> None:
    with open(path, "w") as f:
        json.dump(data, f, indent=4)


def json_to_data(path: Path) -> dict[str, Any]:
    return json.load(path.open())


class StashableBase(BaseModel):
    def to_path(self, path: Path):
        data = self.model_dump()
        match path.suffix:
            case ".json":
                data_to_json(data, path)
            case ".yaml" | ".yml":
                data_to_yaml(data, path)
            case _:
                raise ValueError(f"Unhandled file type {path.suffix}")

    @classmethod
    def from_path(cls, path: Path) -> Self:
        match path.suffix:
            case ".json":
                data = json_to_data(path)
            case ".yaml" | ".yml":
                data = yaml_to_data(path)
            case _:
                raise ValueError(f"Unhandled file type {path.suffix}")
        return cls(**data)


class ProjectBaseModel(StashableBase, use_enum_values=True):
    pass


class TypedEnumType(EnumMeta):
    def __new__(cls, name: str, bases: Any, dct: dict[str, Any]):
        # Go through the type annotations
        annotations = dct.get("__annotations__", {})
        for attr, typ in annotations.items():
            # if typ is annotated, extract the metadata and use that as
            # the function to create the attribute
            if typ in [str, "str"] and attr not in dct:
                # Assign the auto class to the attribute
                dct[attr] = auto()
        for key, value in dct.items():
            if isinstance(value, str):
                if key.lower() == value.lower() and value != value.lower():
                    raise ValueError(f"value {value} is not lowercase version of {key}")
        # Create the class as normal
        return super().__new__(cls, name, bases, dct)  # type: ignore


class StrEnum(str, Enum, metaclass=TypedEnumType):
    """
    Basically members are effectively strings for most purposes.
    the `auto()` shortcut can also be triggered using the str typehint.
    """

    def __new__(cls, *values: str):
        if len(values) > 3:
            raise TypeError("too many arguments for str(): %r" % (values,))
        if len(values) == 1:
            # it must be a string
            if not isinstance(values[0], str):  # type: ignore
                raise TypeError("%r is not a string" % (values[0],))
        if len(values) >= 2:
            # check that encoding argument is a string
            if not isinstance(values[1], str):  # type: ignore
                raise TypeError("encoding must be a string, not %r" % (values[1],))
        if len(values) == 3:
            # check that errors argument is a string
            if not isinstance(values[2], str):  # type: ignore
                raise TypeError("errors must be a string, not %r" % (values[2]))
        value = str(*values)
        member = str.__new__(cls, value)
        member._value_ = value
        return member

    def __str__(self) -> str:
        return str(self.value)

    @staticmethod
    def _generate_next_value_(
        name: str, start: int, count: int, last_values: list[Any]
    ) -> str:
        """
        Return the lower-cased version of the member name.
        """
        return name.lower()

    def __repr__(self) -> str:
        return str.__repr__(self.value)

    @classmethod
    def to_yaml(cls, representer: Any, node: Any):
        return representer.represent_scalar("tag:yaml.org,2002:str", str(node))
