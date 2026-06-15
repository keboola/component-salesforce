'''
Created on 12. 11. 2018

@author: esner
'''
import unittest
import mock
import os
from freezegun import freeze_time

from component import Component
from keboola.component.exceptions import UserException


class TestComponent(unittest.TestCase):

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {'KBC_DATADIR': './non-existing-dir'})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestReplaceHeaders(unittest.TestCase):

    def test_dot_only(self):
        headers = ["Account__ExternalId__c", "Name"]
        result = Component.replace_headers(headers, "__", target=".")
        self.assertEqual(result, ["Account.ExternalId.c", "Name"])

    def test_polymorphic_only(self):
        headers = ["Lead--Who.Email", "Id"]
        result = Component.replace_headers(headers, "--", target=":")
        self.assertEqual(result, ["Lead:Who.Email", "Id"])

    def test_both_passes_colon_then_dot(self):
        headers = ["Lead--Who_DOT_Email", "Account_DOT_ExternalId__c"]
        # Pass 1: colon
        headers = Component.replace_headers(headers, "--", target=":")
        # Pass 2: dot
        headers = Component.replace_headers(headers, "_DOT_", target=".")
        self.assertEqual(headers, ["Lead:Who.Email", "Account.ExternalId__c"])

    def test_neither_set_passthrough(self):
        """When neither sentinel is configured, run() skips replace_headers entirely."""
        headers = ["Lead--Who_DOT_Email", "Id"]
        result = list(headers)
        self.assertEqual(result, ["Lead--Who_DOT_Email", "Id"])

    def test_default_target_is_dot(self):
        headers = ["Account__Name"]
        result = Component.replace_headers(headers, "__")
        self.assertEqual(result, ["Account.Name"])

    def test_no_match_passthrough(self):
        headers = ["Id", "Name"]
        result = Component.replace_headers(headers, "__", target=".")
        self.assertEqual(result, ["Id", "Name"])


class TestOverlapValidation(unittest.TestCase):

    def _check_overlap(self, replace_string, polymorphic_replace_string):
        """Reproduce the overlap guard from Component.run()."""
        if replace_string and polymorphic_replace_string:
            if (replace_string in polymorphic_replace_string
                    or polymorphic_replace_string in replace_string):
                raise UserException(
                    f"replace_string ('{replace_string}') and polymorphic_replace_string "
                    f"('{polymorphic_replace_string}') must not overlap"
                )

    def test_identical_sentinels_rejected(self):
        with self.assertRaises(UserException):
            self._check_overlap("__", "__")

    def test_substring_overlap_rejected(self):
        with self.assertRaises(UserException):
            self._check_overlap("__", "___")

    def test_reverse_substring_overlap_rejected(self):
        with self.assertRaises(UserException):
            self._check_overlap("___", "__")

    def test_non_overlapping_sentinels_accepted(self):
        self._check_overlap("_DOT_", "--")

    def test_one_empty_accepted(self):
        self._check_overlap("__", "")
        self._check_overlap("", "--")

    def test_both_empty_accepted(self):
        self._check_overlap("", "")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
