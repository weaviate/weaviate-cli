import unittest
from semi.commands.misc import _parse_version_from_output


class TestCLI(unittest.TestCase):

    def test_parse_version(self):
        out1 =  """
        Name: weaviate-cli
        Version: 0.1.0rc0
        Summary: Comand line interface to interact with weaviate
        Home-page: UNKNOWN
        Author: SeMI Technologies
        Author-email: hello@semi.technology
        License: UNKNOWN
        Location: /Users/felix/.virtualenvs/testing_cli_4/lib/python3.7/site-packages
        Requires: click, weaviate-client
        Required-by:
        """

        self.assertEqual("0.1.0rc0", _parse_version_from_output(out1))

        out2 = "WARNING: Package(s) not found: weaviate-cli"
        a = "The installed cli version can not be assessed! Run `pip show weaviate-cli` to view the version manually"
        self.assertEqual(a, _parse_version_from_output(out2))
