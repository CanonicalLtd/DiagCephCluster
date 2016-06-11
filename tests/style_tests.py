import os
import pycodestyle
import unittest


class TestCodeFormat(unittest.TestCase):
    def test_conformance(self):
        """Test that we conform to PEP-8."""
        print('Running pep8 tests')
        style = pycodestyle.StyleGuide(quiet=False)
        result = style.check_files(self._get_all_files())
        self.assertEqual(result.total_errors, 0,
                         "Found code style errors (and warnings).")

    def _get_all_files(self):
        rootDir = os.getcwd()
        files = []
        for dirName, subdirList, fileList in os.walk(rootDir):
            parts = dirName.split('/')
            if len(parts) > 1 and parts[-1].startswith('.'):
                subdirList[:] = []
                continue
            for fname in fileList:
                if fname.endswith('.py'):
                    files.append(os.path.join(dirName, fname))
        return files
