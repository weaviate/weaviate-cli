#from unittest.mock import Mock
import unittest.mock as um
import unittest
import os
from modules.configuration import Configuration


class TestConfig(unittest.TestCase):

    def test_is_config_present(self):
        # TODO config does not exist
        # TODO config does exist
        # os_mock = um.Mock()
        # os_mock.os.path.isfile.return_value(False)
        # with um.patch(os, os_mock):

        # patch the home directory
        with um.patch("os.path.isfile", r):
            with um.patch.dict(os.environ, {"HOME": "/home/alice"}):
                # patch the read file
                with um.patch('builtins.open',
                              um.mock_open(read_data='{"A":"B"}')):

                    cfg = Configuration()
