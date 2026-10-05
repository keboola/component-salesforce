import logging
import unittest

from component import DEFAULT_API_VERSION, resolve_api_version


class TestResolveApiVersion(unittest.TestCase):
    """Saved configurations can hold an API version that Salesforce has retired."""

    def test_missing_version_uses_default(self):
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(None))

    def test_empty_version_uses_default(self):
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(""))
        self.assertEqual(DEFAULT_API_VERSION, resolve_api_version("  "))

    def test_retired_version_is_replaced_and_logged(self):
        for retired in ("21.0", "25.0", "30.0"):
            with self.subTest(retired=retired):
                with self.assertLogs(level=logging.WARNING) as logs:
                    self.assertEqual(DEFAULT_API_VERSION, resolve_api_version(retired))
                self.assertIn(retired, "".join(logs.output))

    def test_supported_version_is_kept(self):
        for supported in ("31.0", "40.0", "58.0", "64.0"):
            with self.subTest(supported=supported):
                self.assertEqual(supported, resolve_api_version(supported))

    def test_supported_version_logs_no_warning(self):
        with self.assertNoLogs(level=logging.WARNING):
            resolve_api_version("58.0")

    def test_non_numeric_version_is_kept_for_salesforce_to_reject(self):
        self.assertEqual("not-a-version", resolve_api_version("not-a-version"))


if __name__ == "__main__":
    unittest.main()
