from pathlib import Path
from typing import Any, Generic, Type, TypeVar, cast

from pyarrow import Table  # type: ignore
from ruamel.yaml import YAML

from ..types import BaseModelLike, PyArrowLike

T = TypeVar("T", bound="BaseModelLike")


class YamlData(Generic[T]):
    """
    YAMLs aren't processed directly by duckdb, so we need to load them into a
    arrow table ourselves before passing it in as a memory object.
    """

    validation_model: Type[T]
    yaml_source: Path
    data_class: PyArrowLike = cast(PyArrowLike, Table)

    @classmethod
    def get_data_solo(cls) -> list[dict[str, Any]]:
        yaml = YAML(typ="safe")
        yaml_data = yaml.load(cls._get_yaml_source())
        return yaml_data

    @classmethod
    def _get_yaml_source(cls) -> Path:
        return cls.yaml_source

    @classmethod
    def get_validation_class(cls) -> Type[T]:
        # retrieve the validation class based on whatever was passed to specify the
        # Generic
        # e.g. if this class has been subclassed as YamlData[Policy], then return Policy

        if hasattr(cls, "validation_model"):
            return cls.validation_model
        else:
            return cls.__orig_bases__[0].__args__[0]  # type: ignore

    @classmethod
    def get_data_folder(cls) -> list[dict[str, Any]]:
        # as yaml_source is a glob for a folder, we need to loop through the files
        # and return a list of dicts

        parent_folder = cls.yaml_source.parent
        just_filename = cls.yaml_source.name

        data: list[dict[str, Any]] = []
        for file in parent_folder.glob(just_filename):
            yaml = YAML(typ="safe")
            yaml_data = yaml.load(file)
            if isinstance(yaml_data, list):
                data.extend(yaml_data)
            else:
                data.append(yaml_data)
        return data

    @classmethod
    def get_data(cls) -> list[dict[str, Any]]:
        just_file_name = cls.yaml_source.name

        # check suffix is yaml or yml and give error if not
        if just_file_name.endswith(".yaml") or just_file_name.endswith(".yml"):
            pass
        else:
            raise ValueError(
                f"YamlData yaml_source must end with .yaml or .yml, got {just_file_name}"
            )

        if just_file_name == "*.yaml" or just_file_name == "*.yml":
            return cls.get_data_folder()
        else:
            return cls.get_data_solo()

    @classmethod
    def validated_models(cls) -> list[T]:
        new_data: list[T] = []
        yaml_data = cls.get_data()
        model = cls.get_validation_class()
        for item in yaml_data:
            new_data.append(model.model_validate(item))
        return new_data

    @classmethod
    def post_validation(cls, models: list[T]) -> list[dict[str, Any]]:
        return [model.model_dump() for model in models]

    @classmethod
    def get_source(cls) -> PyArrowLike:
        validated_models = cls.validated_models()
        data = cls.post_validation(validated_models)
        df = cls.data_class.from_pylist(data)
        return df
