"""Tests for pipeline logging configuration."""

import logging
from datetime import UTC, datetime

import pytest

from pipeline.logger import setup_logging


def _console_handlers(logger: logging.Logger) -> list[logging.Handler]:
    """Our own console handlers, excluding pytest's caplog handler and file handlers."""
    return [
        h
        for h in logger.handlers
        if isinstance(h, logging.StreamHandler)
        and not isinstance(h, logging.FileHandler)
        and "LogCapture" not in h.__class__.__name__
    ]


def _file_handlers(logger: logging.Logger) -> list[logging.FileHandler]:
    return [h for h in logger.handlers if isinstance(h, logging.FileHandler)]


class TestSetupLogging:
    """Test setup_logging functionality."""

    @pytest.mark.parametrize("console_level", [logging.INFO, logging.WARNING])
    def test_console_only_logging(self, caplog, console_level: int):
        """Without a log file there is exactly one console handler, at the level asked for."""
        caplog.set_level(logging.INFO)

        logger = setup_logging(log_file=None, console_level=console_level)

        assert logger is logging.getLogger()
        # setup_logging pins the root logger to DEBUG and filters at the handlers.
        assert logger.level == logging.DEBUG

        console = _console_handlers(logger)
        assert len(console) == 1
        assert console[0].level == console_level
        assert _file_handlers(logger) == []

        logger.info("Test message")
        assert "Test message" in caplog.text

    def test_file_logging(self, tmp_path):
        """The log file receives the message, formatted as timestamp | level | message."""
        log_file = tmp_path / "test.log"

        logger = setup_logging(log_file=log_file, file_level=logging.DEBUG)

        assert len(_file_handlers(logger)) == 1
        assert _file_handlers(logger)[0].level == logging.DEBUG

        logger.info("Test message to file")
        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert " | INFO | Test message to file" in content
        assert content.startswith(datetime.now(UTC).strftime("%Y-%m-"))

    def test_an_existing_log_file_is_appended_to_not_recreated(self, tmp_path):
        """Setup only creates the parent directory when the file is not there yet.

        Pointing at a file that already exists takes the other branch, and the
        earlier content has to survive: a run that silently truncated the log
        would lose the record of the run before it.
        """
        log_file = tmp_path / "existing.log"
        log_file.write_text("earlier run\n", encoding="utf-8")

        logger = setup_logging(log_file=log_file, log_file_mode="a")
        logger.info("later run")
        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "earlier run" in content
        assert " | INFO | later run" in content

    def test_repeated_setup_replaces_handlers(self, tmp_path, caplog):
        """Repeat calls swap handlers rather than accumulate, even for a new file."""
        caplog.set_level(logging.INFO)
        log_file1 = tmp_path / "test1.log"
        log_file2 = tmp_path / "test2.log"

        setup_logging(log_file=log_file1)
        logger = setup_logging(log_file=log_file2)

        assert len(_console_handlers(logger)) == 1
        file_handlers = _file_handlers(logger)
        assert len(file_handlers) == 1
        assert file_handlers[0].baseFilename == str(log_file2)

        logger.info("Message to file 2")
        for handler in logger.handlers:
            handler.flush()

        assert "Message to file 2" in log_file2.read_text(encoding="utf-8")
        assert "Message to file 2" not in log_file1.read_text(encoding="utf-8")
        assert "Message to file 2" in caplog.text

    def test_preserves_existing_handlers(self, caplog):
        """Test that setup_logging preserves pytest's caplog handler."""
        caplog.set_level(logging.INFO)

        setup_logging(log_file=None)

        logging.getLogger().info("Test message after setup")
        assert "Test message after setup" in caplog.text

    def test_unicode_characters_in_logs(self, tmp_path, caplog):
        """The status glyphs survive to both the captured stream and the UTF-8 file."""
        caplog.set_level(logging.INFO)
        log_file = tmp_path / "test.log"

        logger = setup_logging(log_file=log_file)
        logger.info("Status: ✓ CACHED, ✗ NO CACHE, ∅ DISABLED")

        assert "Status: ✓ CACHED, ✗ NO CACHE, ∅ DISABLED" in caplog.text

        for handler in logger.handlers:
            handler.flush()

        content = log_file.read_text(encoding="utf-8")
        assert "Status: ✓ CACHED, ✗ NO CACHE, ∅ DISABLED" in content
