from collections import defaultdict
from typing import Any, cast


def get_name(obj: Any) -> str:
    return cast(str, obj.__name__)


def nested_dict() -> defaultdict[str, Any]:
    return defaultdict(nested_dict)


def defaultdict_to_normal(di: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in di.items():
        if isinstance(v, defaultdict):
            out[k] = defaultdict_to_normal(v)
        else:
            out[k] = v
    return out


def unnest(di: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = nested_dict()

    for k, v in di.items():
        current_dict = out
        levels = k.split("__")
        level_key, final = levels[:-1], levels[-1]
        for level in level_key:
            current_dict = current_dict[level]
        current_dict[final] = v

    return defaultdict_to_normal(out)
