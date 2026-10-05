import logging
import unittest

from component import (
    DEFAULT_API_VERSION,
    MIN_BULK1_API_VERSION,
    MIN_BULK2_API_VERSION,
    resolve_api_version,
)


class TestResolveApiVersion(unittest.TestCase):
    """A saved configuration can hold an API version that the selected write path cannot use."""

    def test_default_is_usable_by_both_write_paths(self):
        self.assertEqual("64.0", DEFAULT_API_VERSION)
        self.assertGreaterEqual(float(DEFAULT_API_VERSION), MIN_BULK2_API_VERSION)
        self.assertGreaterEqual(float(DEFAULT_API_VERSION), MIN_BULK1_API_VERSION)

    def test_missing_version_uses_default(self):
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(None))
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(None, serial_mode=True))

    def test_empty_version_uses_default(self):
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(""))
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version("  "))

    def test_retired_version_is_replaced_and_logged(self):
        for retired in ("21.0", "25.0", "30.0"):
            for serial_mode in (False, True):
                with self.subTest(retired=retired, serial_mode=serial_mode):
                    with self.assertLogs(level=logging.WARNING) as logs:
                        resolved = resolve_api_version(retired, serial_mode=serial_mode)
                    self.assertEqual(DEFAULT_API_VERSION, resolved)
                    self.assertIn(retired, "".join(logs.output))

    def test_bulk2_run_replaces_a_version_below_41(self):
        # Bulk API 2.0 ingest does not exist below 41.0, so such a run fails today.
        for too_low in ("31.0", "35.0", "40.0"):
            with self.subTest(too_low=too_low):
                with self.assertLogs(level=logging.WARNING):
                    self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(too_low))

    def test_serial_run_keeps_a_version_between_31_and_40(self):
        # Serial mode uses Bulk API 1.0, which still works on these versions. Do not change it.
        for supported in ("31.0", "35.0", "40.0"):
            with self.subTest(supported=supported):
                with self.assertNoLogs(level=logging.WARNING):
                    self.assertEqual(supported, resolve_api_version(supported, serial_mode=True))

    def test_supported_version_is_kept(self):
        for supported in ("41.0", "58.0", "64.0"):
            for serial_mode in (False, True):
                with self.subTest(supported=supported, serial_mode=serial_mode):
                    with self.assertNoLogs(level=logging.WARNING):
                        self.assertEqual(supported, resolve_api_version(supported, serial_mode=serial_mode))

    def test_non_numeric_version_is_kept_for_salesforce_to_reject(self):
        self.assertEqual("not-a-version", resolve_api_version("not-a-version"))


class TestSecurityTokenDeprecation(unittest.TestCase):

    def test_warning_names_the_replacement_login_method(self):
        from component import SECURITY_TOKEN_DEPRECATION_WARNING
        self.assertIn("login()", SECURITY_TOKEN_DEPRECATION_WARNING)
        self.assertIn("Connected app OAuth 2.0 client credentials", SECURITY_TOKEN_DEPRECATION_WARNING)


if __name__ == "__main__":
    unittest.main()
