import sys
from pathlib import Path

sys.path.append(Path(__file__).parents[1] / "utils")

from unittest.mock import patch  # noqa: E402

import move_to_sheets  # pylint: disable=all # noqa: E402
from db_utils.utils import get_environment  # noqa: E402


def test_move_to_sheets():
    """
    Test moving data to Google Sheets
    """
    if get_environment() == "LOCAL":
        with patch("sys.argv", ["move_to_sheets.py"]):
            move_to_sheets.main()
            assert True
    else:
        assert True
