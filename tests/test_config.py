from pathlib import Path

import pytest

from bridge.config import AppConfig, ConfigError, load_config

BASE_YAML = "webui:\n  username: admin\n"
ARR_YAML = "arr:\n  radarr:\n    url: http://r:7878\n  sonarr:\n    url: http://s:8989\n"
MIN_YAML = BASE_YAML + ARR_YAML
KEYS = {"RADARR_API_KEY": "fake-r", "SONARR_API_KEY": "fake-s"}


def _cfg(tmp_path: Path, yaml_text: str = MIN_YAML, **env: str) -> AppConfig:
    p = tmp_path / "config.yml"
    p.write_text(yaml_text, encoding="utf-8")
    return load_config({"WEBUI_PASSWORD": "fake-pass", "BRIDGE_CONFIG": str(p), **KEYS, **env})


def test_defaults_match_design_table(tmp_path: Path) -> None:
    c = _cfg(tmp_path)
    s = c.settings
    assert s.webui.port == 8080
    assert s.paths.root == "/data/downloads/115"
    assert s.paths.incomplete == "/data/downloads/115/.incomplete"
    assert s.paths.state == "/config"
    assert s.categories == {"radarr": "radarr", "tv-sonarr": "sonarr"}
    assert set(s.arr) == {"radarr", "sonarr"}
    assert s.cloud.root_dir == "/115-bridge"
    assert s.cloud.timeout_hours == 12
    assert s.cloud.poll_seconds == 60
    assert s.cloud.quota_min == 20
    assert s.cloud.space_min_gib == 100
    assert s.api.rate_per_second == 1
    assert s.api.burst == 3
    assert s.fetch.connections_per_file == 2
    assert s.fetch.max_files == 1
    assert s.fetch.download_app == "android"
    assert s.fetch.min_free_gib == 50
    assert s.fetch.max_retries == 5
    assert s.verify.max_retries == 2
    assert (c.runtime.puid, c.runtime.pgid, c.runtime.umask) == (1024, 100, "000")
    assert c.secrets.telegram_bot_token is None
    assert not c.secrets.telegram_enabled


def test_missing_username_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"webui\.username"):
        _cfg(tmp_path, ARR_YAML)


def test_overrides_and_runtime_env(tmp_path: Path) -> None:
    c = _cfg(
        tmp_path,
        MIN_YAML + "cloud:\n  quota_min: 5\n",
        PUID="1000",
        PGID="10",
        UMASK="022",
    )
    assert c.settings.cloud.quota_min == 5
    assert (c.runtime.puid, c.runtime.pgid, c.runtime.umask) == (1000, 10, "022")


def test_missing_password_names_variable(tmp_path: Path) -> None:
    p = tmp_path / "c.yml"
    p.write_text(MIN_YAML, encoding="utf-8")
    with pytest.raises(ConfigError, match="WEBUI_PASSWORD"):
        load_config({"BRIDGE_CONFIG": str(p)})


def test_arr_requires_api_key_and_error_has_no_values(tmp_path: Path) -> None:
    y = MIN_YAML
    p = tmp_path / "config.yml"
    p.write_text(y, encoding="utf-8")
    with pytest.raises(ConfigError) as ei:
        load_config(
            {
                "WEBUI_PASSWORD": "fake-pass",
                "BRIDGE_CONFIG": str(p),
                "RADARR_API_KEY": "fake-radarr-key",
            }
        )
    msg = str(ei.value)
    assert "SONARR_API_KEY" in msg
    assert "RADARR_API_KEY" not in msg
    assert "fake-radarr-key" not in msg
    assert "fake-pass" not in msg
    c = _cfg(tmp_path, y, RADARR_API_KEY="k1", SONARR_API_KEY="k2")
    assert {k: v.get_secret_value() for k, v in c.secrets.arr_api_keys.items()} == {
        "radarr": "k1",
        "sonarr": "k2",
    }


@pytest.mark.parametrize(
    "snippet",
    [
        "webui:\n  username: a\n  password: fakeval\n",
        "arr:\n  radarr:\n    url: http://r\n    api_key: fakeval\n",
        "telegram:\n  bot_token: fakeval\n",
        "telegram_chat_id: 1\n",
    ],
)
def test_secrets_in_yaml_rejected(tmp_path: Path, snippet: str) -> None:
    with pytest.raises(ConfigError, match="environment variables") as ei:
        _cfg(tmp_path, snippet)
    assert "fakeval" not in str(ei.value)


def test_unknown_key_rejected(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        _cfg(tmp_path, MIN_YAML + "bogus: 1\n")


def test_telegram_optional_enabled_when_both_set(tmp_path: Path) -> None:
    assert not _cfg(tmp_path).secrets.telegram_enabled
    c = _cfg(tmp_path, TELEGRAM_BOT_TOKEN="1:fake", TELEGRAM_CHAT_ID="42")
    assert c.secrets.telegram_enabled
    assert "fake" not in repr(c.secrets)


def test_bad_puid(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="PUID"):
        _cfg(tmp_path, PUID="abc")


@pytest.mark.parametrize(
    ("env", "missing"),
    [
        ({"TELEGRAM_BOT_TOKEN": "1:fake"}, "TELEGRAM_CHAT_ID"),
        ({"TELEGRAM_CHAT_ID": "424242"}, "TELEGRAM_BOT_TOKEN"),
    ],
)
def test_telegram_half_configured_rejected(
    tmp_path: Path, env: dict[str, str], missing: str
) -> None:
    with pytest.raises(ConfigError, match=missing) as ei:
        _cfg(tmp_path, **env)
    msg = str(ei.value)
    assert "1:fake" not in msg
    assert "424242" not in msg


def test_category_pointing_to_undefined_arr_rejected(tmp_path: Path) -> None:
    y = BASE_YAML + "arr:\n  radarr:\n    url: http://r:7878\n"
    with pytest.raises(ConfigError) as ei:
        _cfg(tmp_path, y)
    msg = str(ei.value)
    assert "tv-sonarr" in msg
    assert "sonarr" in msg


def test_missing_config_file_is_explicit_error(tmp_path: Path) -> None:
    missing = tmp_path / "nope" / "c.yml"
    with pytest.raises(ConfigError, match="找不到配置文件") as ei:
        load_config({"WEBUI_PASSWORD": "fake-pass", "BRIDGE_CONFIG": str(missing)})
    assert str(missing) in str(ei.value)


def test_default_categories_without_arr_fail(tmp_path: Path) -> None:
    # 文件存在但没有 arr：默认 categories 指向未定义的 Arr
    with pytest.raises(ConfigError, match="categories"):
        _cfg(tmp_path, BASE_YAML)


def test_secrets_not_in_any_serialization(tmp_path: Path) -> None:
    c = _cfg(tmp_path, TELEGRAM_BOT_TOKEN="1:fake-tg", TELEGRAM_CHAT_ID="fake-chat")
    sec = c.secrets
    outputs = [
        repr(sec),
        str(sec),
        str(sec.model_dump()),
        sec.model_dump_json(),
        str(c.model_dump()),
        c.model_dump_json(),
        repr(c),
    ]
    for out in outputs:
        for fake in ("fake-pass", "fake-r", "fake-s", "1:fake-tg", "fake-chat"):
            assert fake not in out
    assert sec.webui_password.get_secret_value() == "fake-pass"
    assert sec.arr_api_keys["radarr"].get_secret_value() == "fake-r"
    assert sec.telegram_bot_token is not None
    assert sec.telegram_bot_token.get_secret_value() == "1:fake-tg"
    assert sec.telegram_enabled


def test_arr_type_inferred_from_name(tmp_path: Path) -> None:
    c = _cfg(tmp_path)
    assert c.settings.arr["radarr"].resolved_type("radarr") == "radarr"
    assert c.settings.arr["sonarr"].resolved_type("sonarr") == "sonarr"


def test_arr_type_explicit_overrides_name(tmp_path: Path) -> None:
    y = (
        BASE_YAML
        + "arr:\n  movies:\n    url: http://r\n    type: radarr\n"
        + "  tv:\n    url: http://s\n    type: sonarr\n"
        + "categories:\n  radarr: movies\n  tv-sonarr: tv\n"
    )
    c = _cfg(tmp_path, y, MOVIES_API_KEY="k1", TV_API_KEY="k2")
    assert c.settings.arr["movies"].resolved_type("movies") == "radarr"
    assert c.settings.arr["tv"].resolved_type("tv") == "sonarr"


def test_arr_type_uninferable_rejected(tmp_path: Path) -> None:
    y = BASE_YAML + "arr:\n  movies:\n    url: http://r\ncategories:\n  radarr: movies\n"
    with pytest.raises(ConfigError, match="movies") as ei:
        _cfg(tmp_path, y, MOVIES_API_KEY="fake-mk")
    assert "fake-mk" not in str(ei.value)


def test_arr_type_invalid_value_rejected(tmp_path: Path) -> None:
    y = BASE_YAML + "arr:\n  radarr:\n    url: http://r\n    type: lidarr\n"
    with pytest.raises(ConfigError):
        _cfg(tmp_path, y)
