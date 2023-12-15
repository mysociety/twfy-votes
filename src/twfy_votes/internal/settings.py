import os
from pathlib import Path
from typing import Annotated

from pydantic import Field
from pydantic.functional_validators import AfterValidator
from pydantic_settings import BaseSettings, SettingsConfigDict


def is_existing_path(p: Path) -> Path:
    if not p.exists():
        raise ValueError(f"{p} does not exist")
    return p


RealPath = Annotated[Path, AfterValidator(is_existing_path)]

src_path = Path(__file__).parent.parent
top_level = src_path.parent.parent


# construct codespaces url
if os.environ.get("CODESPACES", None) == "true":
    name = os.environ["CODESPACE_NAME"]
    port = 8000
    domain = os.environ["GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN"]
    base_url = f"https://{name}-{port}.{domain}"
else:
    base_url = "http://localhost:8000"


class Settings(BaseSettings):
    # the below can be overriden by .env file
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")
    site_name: str = "TheyWorkForYou Votes"
    template_dir: RealPath = src_path / "templates"
    static_dir: RealPath = top_level / "static"
    render_dir: Path = top_level / "_site"
    base_url: str = base_url
    server_production: bool = Field(default=False, alias="SERVER_PRODUCTION")
    twfy_api_key: str = Field(default=None, alias="TWFY_API_KEY")


settings = Settings()
