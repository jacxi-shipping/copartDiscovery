"""Logging configuration helper."""

from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO", *, json_logs: bool = False) -> None:
    """
    Set up a console logger.

    Parameters
    ----------
    level:
        Logging verbosity (DEBUG / INFO / WARNING / ERROR).
    json_logs:
        When ``True``, emit structured JSON log lines (requires
        ``python-json-logger``).  Falls back to plain text if the package
        is not installed.
    """
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)

    if json_logs:
        try:
            from pythonjsonlogger.jsonlogger import JsonFormatter  # type: ignore[import-untyped]

            handler.setFormatter(
                JsonFormatter(
                    fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )
        except ImportError:
            logging.warning(
                "python-json-logger is not installed; falling back to plain text logging"
            )
            handler.setFormatter(
                logging.Formatter(
                    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                    datefmt="%Y-%m-%dT%H:%M:%S",
                )
            )
    else:
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        )

    logging.basicConfig(level=numeric_level, handlers=[handler])
