# pylint: disable=duplicate-code

import json
import sys
from pathlib import Path
from typing import Callable, Dict


def get_cfg() -> Dict[str, str]:
    """
    Loads config: dbt objects & their location

    Returns:
        dict[str, str]: Loaded config
    """
    with open(f"_cfg_{Path(__file__).stem}.json", "r", encoding="utf-8") as f:
        return json.load(f)


def get_conventions() -> Dict[str, Callable[[str], bool]]:
    """
    Returns dict of conventions to check if file names are prefixed correctly

    Conventions specify whether a file name adheres to prefix rules (based on file extension)

    Returns:
        Dict[str, Callable[[str], bool]]: Dict where keys are file extensions
        & values are functions that take a file name as input & return a bool
        indicating if file name meets convention (based on file extension)

    """
    return {
        ".csv": lambda name: not name.startswith("_"),
        ".md": lambda name: name.startswith("__") and not name.startswith("___"),
        ".sql": lambda name: not name.startswith("_"),
        ".yml": lambda name: name.startswith("_") and not name.startswith("__"),
    }


def main():
    """
    Checks if any dbt object does not follow naming conventions
    """

    obj_paths = get_cfg()
    conventions = get_conventions()
    exceptions = ["unit_tests.yml", "sources.yml"]

    errors = []
    for o_type, p in obj_paths.items():
        for f in Path(p).glob("**/*"):
            if f.is_file():
                check = conventions.get(f.suffix)
                if not check:
                    print(f"No naming conv defined for file `{f.name}` of type `{o_type}`")
                    sys.exit(1)

                if not check(f.name) and f.name not in exceptions:
                    err_msg = f"Object `{f.name}` of type `{o_type}` violates naming conv"
                    errors.append(err_msg)

    if errors:
        print("\n".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
