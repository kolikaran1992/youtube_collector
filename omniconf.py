from dynaconf import Dynaconf
from datetime import datetime
import pytz
from pathlib import Path
import logging, os


_NOW = datetime.now()
_BASE_DIR = Path(__file__).resolve().parent.parent


class DefaultFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        super().__init__(fmt=fmt, datefmt=datefmt)

    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, pytz.timezone(config.get("tz")))
        if datefmt:
            return dt.strftime(datefmt)
        return dt.isoformat()

    def format(self, record):
        # Ensure full file path is included
        record.full_path = record.pathname  # full absolute path of the source file
        return super().format(record)


def get_logger() -> logging.Logger:
    name = "yt_collector"
    logger = logging.getLogger(name)

    logger.setLevel(getattr(logging, "INFO"))

    # Include log_file_path in the format string
    fmt = "[%(asctime)s] %(levelname)s [%(full_path)s]: %(message)s"
    formatter = DefaultFormatter(fmt=fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logger.level)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logger._initialized = True

    return logger


def _get_start_ts(tz: str) -> datetime:
    return _NOW.astimezone(pytz.timezone(tz))


def _get_now_iso(tz: str) -> str:
    return datetime.now().astimezone(pytz.timezone(tz)).isoformat()


def _get_now_ts(tz: str) -> str:
    return datetime.now().astimezone(pytz.timezone(tz))


logger = get_logger()
secrets_dir = os.environ.get("SECRETS_DIRECTORY", None)
config = Dynaconf(
    preload=["settings_file/settings.toml"],
    settings_files=["settings_file/yt_collector.toml"],
    secrets=[] if not secrets_dir else list(Path(secrets_dir).glob("*.toml")),
    # to enable overriding of single variables at runtime
    environments=True,
    envvar_prefix="YT_COLLECTOR",
    # to enable merging of user defined and base settings
    load_dotenv=True,
    # jinja variables
    _get_now_ts=_get_now_ts,
    _get_now_iso=_get_now_iso,
    _get_start_ts=_get_start_ts,
    now=_NOW,
    partition_date=_NOW.strftime("%Y/%m/%d"),
    root_dir=_BASE_DIR.as_posix(),
    home_dir=Path.home().as_posix(),
    merge_enabled=True,
)
