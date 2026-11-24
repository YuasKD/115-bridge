"""Configuration loading: YAML file plus environment variables.

Secrets (WebUI password, Arr API keys, Telegram token/chat id) are read from the
environment only. A YAML file that contains a secret-looking key is rejected.
Error messages name variables or keys, never values.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError

DEFAULT_CONFIG_PATH = "/config/config.yml"
CONFIG_ENV = "BRIDGE_CONFIG"

_SECRET_KEY_RE = re.compile(
    r"(password|passwd|secret|token|api_?key|chat_?id|cookie)", re.IGNORECASE
)


class ConfigError(Exception):
    """Raised for any invalid or incomplete configuration."""


class _Section(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class WebUIConfig(_Section):
    port: int = 8080
    username: str | None = None
    history_retention_days: int = Field(default=7, ge=0)


class PathsConfig(_Section):
    root: str = "/data/downloads/115"
    incomplete: str = "/data/downloads/115/.incomplete"
    state: str = "/config"


class CloudConfig(_Section):
    root_dir: str = "/115-bridge"
    timeout_hours: int = 12
    poll_seconds: int = 60
    quota_min: int = 20
    space_min_gib: int = 100


class ApiConfig(_Section):
    rate_per_second: float = 1
    burst: int = 3


class FetchConfig(_Section):
    connections_per_file: int = 2
    max_files: int = 1
    download_app: str = "android"
    min_free_gib: int = 50
    max_retries: int = 5


class VerifyConfig(_Section):
    max_retries: int = 2


ArrType = Literal["radarr", "sonarr"]


class ArrConfig(_Section):
    url: str
    # Implementation detail, not in the design doc table: explicit Arr type.
    # When None the type is inferred from the Arr name.
    type: ArrType | None = None

    def resolved_type(self, name: str) -> ArrType:
        """Final type: explicit ``type`` or inferred from ``name``."""
        if self.type is not None:
            return self.type
        lowered = name.lower()
        has_radarr = "radarr" in lowered
        has_sonarr = "sonarr" in lowered
        if has_radarr and not has_sonarr:
            return "radarr"
        if has_sonarr and not has_radarr:
            return "sonarr"
        raise ConfigError(f"cannot determine Arr type for {name!r}; set arr.{name}.type")


def _default_categories() -> dict[str, str]:
    return {"radarr": "radarr", "tv-sonarr": "sonarr"}


class Settings(_Section):
    webui: WebUIConfig = Field(default_factory=WebUIConfig)
    paths: PathsConfig = Field(default_factory=PathsConfig)
    categories: dict[str, str] = Field(default_factory=_default_categories)
    arr: dict[str, ArrConfig] = Field(default_factory=dict)
    cloud: CloudConfig = Field(default_factory=CloudConfig)
    api: ApiConfig = Field(default_factory=ApiConfig)
    fetch: FetchConfig = Field(default_factory=FetchConfig)
    verify: VerifyConfig = Field(default_factory=VerifyConfig)


class Secrets(BaseModel):
    model_config = ConfigDict(frozen=True)

    webui_password: SecretStr
    arr_api_keys: dict[str, SecretStr] = Field(default_factory=dict)
    telegram_bot_token: SecretStr | None = None
    telegram_chat_id: SecretStr | None = None

    def __repr__(self) -> str:
        return "Secrets(<redacted>)"

    __str__ = __repr__

    @property
    def telegram_enabled(self) -> bool:
        return bool(
            self.telegram_bot_token
            and self.telegram_bot_token.get_secret_value()
            and self.telegram_chat_id
            and self.telegram_chat_id.get_secret_value()
        )


class Runtime(BaseModel):
    model_config = ConfigDict(frozen=True)

    puid: int = 1024
    pgid: int = 100
    umask: str = "000"


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    settings: Settings
    secrets: Secrets
    runtime: Runtime


def arr_env_name(name: str) -> str:
    """Environment variable holding the API key of the Arr called ``name``."""
    return re.sub(r"[^A-Za-z0-9]", "_", name).upper() + "_API_KEY"


def _find_secret_keys(data: Any, prefix: str = "") -> list[str]:
    found: list[str] = []
    if isinstance(data, dict):
        for key, value in data.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if _SECRET_KEY_RE.search(str(key)):
                found.append(path)
            else:
                found.extend(_find_secret_keys(value, path))
    elif isinstance(data, list):
        for i, item in enumerate(data):
            found.extend(_find_secret_keys(item, f"{prefix}[{i}]"))
    return found


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"找不到配置文件：{path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError:
        raise ConfigError(f"config file {path} is not valid YAML") from None
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"config file {path} must contain a mapping at top level")
    leaked = _find_secret_keys(raw)
    if leaked:
        raise ConfigError(
            "secrets must come from environment variables, not the config file; "
            f"remove keys: {', '.join(leaked)}"
        )
    return raw


def _int_env(env: Mapping[str, str], name: str, default: int) -> int:
    value = env.get(name)
    if value is None or value == "":
        return default
    try:
        return int(value)
    except ValueError:
        raise ConfigError(f"environment variable {name} must be an integer") from None


def load_config(env: Mapping[str, str] | None = None) -> AppConfig:
    """Load settings from YAML (path from ``BRIDGE_CONFIG``) and secrets from env."""
    environ: Mapping[str, str] = os.environ if env is None else env
    path = Path(environ.get(CONFIG_ENV) or DEFAULT_CONFIG_PATH)
    raw = _load_yaml(path)
    try:
        settings = Settings.model_validate(raw)
    except ValidationError as exc:
        problems = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}" for e in exc.errors()
        )
        raise ConfigError(f"invalid configuration: {problems}") from None

    dangling = [
        f"{cat} -> {arr}" for cat, arr in settings.categories.items() if arr not in settings.arr
    ]
    if dangling:
        raise ConfigError(
            "categories refer to Arr names not defined under arr: " + ", ".join(dangling)
        )

    for arr_name, arr_cfg in settings.arr.items():
        arr_cfg.resolved_type(arr_name)

    tg_vars = ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID")
    tg_missing = [v for v in tg_vars if not environ.get(v)]
    if len(tg_missing) == 1:
        raise ConfigError(
            f"Telegram needs both {tg_vars[0]} and {tg_vars[1]}; missing {tg_missing[0]}"
        )

    missing: list[str] = []
    if not environ.get("WEBUI_PASSWORD"):
        missing.append("WEBUI_PASSWORD")
    if not settings.webui.username:
        missing.append("webui.username (config file)")
    arr_keys: dict[str, SecretStr] = {}
    for name in settings.arr:
        var = arr_env_name(name)
        key = environ.get(var)
        if key:
            arr_keys[name] = SecretStr(key)
        else:
            missing.append(var)
    if missing:
        raise ConfigError("missing required configuration: " + ", ".join(missing))

    secrets = Secrets(
        webui_password=SecretStr(environ["WEBUI_PASSWORD"]),
        arr_api_keys=arr_keys,
        telegram_bot_token=SecretStr(environ["TELEGRAM_BOT_TOKEN"])
        if environ.get("TELEGRAM_BOT_TOKEN")
        else None,
        telegram_chat_id=SecretStr(environ["TELEGRAM_CHAT_ID"])
        if environ.get("TELEGRAM_CHAT_ID")
        else None,
    )
    runtime = Runtime(
        puid=_int_env(environ, "PUID", 1024),
        pgid=_int_env(environ, "PGID", 100),
        umask=environ.get("UMASK") or "000",
    )
    return AppConfig(settings=settings, secrets=secrets, runtime=runtime)
