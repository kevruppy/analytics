import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Dict, List

import yaml


def get_cfg() -> Dict[str, str]:
    """
    Loads config: dbt objects & their location

    Returns:
        dict[str, str]: Loaded config
    """
    with open(f"_cfg_{Path(__file__).stem}.json", "r", encoding="utf-8") as f:
        return json.load(f)


def check_obj_lvl_tests(obj_paths: Dict[str, str]) -> List[str]:
    """
    Validates object level test names for models, seeds & snapshots

    Params:
        obj_paths (dict[str, str]): Contains mapping between dbt objects & their location

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []
    for o_type, paths in obj_paths.items():
        for p in paths:
            for f in Path(p).glob("*.yml"):
                with open(f, "r", encoding="utf-8") as yml:
                    props = yaml.safe_load(yml)

                for dt in props[o_type][0].get("data_tests", {}):
                    t_type = next(iter(dt.keys()))
                    act_t_name = dt[t_type]["name"]
                    adj_t_name = re.sub(r"_\d$", "", act_t_name)

                    if adj_t_name != "__".join(
                        [props[o_type][0]["name"], t_type.replace(".", "_dot_")]
                    ):
                        errors.append(f"{o_type} obj lvl test violates naming convs: {act_t_name}")
    return errors


def check_sources_col_lvl_test_names(path_to_sources_yml: str) -> List[str]:
    """
    Validates column level test names for sources

    Params:
        path_to_sources_yml (str): Path to sources.yml

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    with open(path_to_sources_yml, "r", encoding="utf-8") as yml:
        props = yaml.safe_load(yml)

    errors = []
    for p in props["sources"][0]["tables"]:
        tbl = p["name"]
        for c in p["columns"]:
            for dt in c.get("data_tests", {}):
                t_type = next(iter(dt.keys()))
                act_t_name = dt[t_type]["name"]
                exp_t_name = f"""src__{tbl}__{c["name"].lower()}__{t_type}"""

                if act_t_name != exp_t_name:
                    errors.append(f"sources col lvl test violates naming convs: {act_t_name}")
    return errors


def check_mod_sed_snp_col_lvl_test_names(obj_paths: Dict[str, str]) -> List[str]:
    """
    Validates column level test names for models, seeds & snapshots

    Params:
        obj_paths (dict[str, str]): Contains mapping between dbt object types & their location

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []
    for o_type, paths in obj_paths.items():
        if o_type != "sources":
            for p in paths:
                for f in Path(p).glob("*.yml"):
                    with open(f, "r", encoding="utf-8") as yml:
                        props = yaml.safe_load(yml)
                        for c in props[o_type][0]["columns"]:
                            col = c["name"].lower()
                            for dt in c.get("data_tests", {}):
                                t_type = next(iter(dt.keys()))
                                act_t_name = dt[t_type]["name"]
                                adj_t_type = t_type.replace(".", "_dot_")
                                exp_t_name = f"""{props["name"]}__{col}__{adj_t_type}"""

                                if act_t_name != exp_t_name:
                                    errors.append(
                                        f"{o_type} col lvl test violates naming convs: {act_t_name}"
                                    )
    return errors


def check_uniqueness_of_test_names(obj_paths: Dict[str, str]) -> List[str]:
    """
    Checks if dbt test names are unique

    Params:
        obj_paths (dict[str, str]): Contains mapping between dbt objects & their location

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    exceptions = ["unit_tests.yml"]
    test_names = []
    errors = []

    for o_type, p in obj_paths.items():  # pylint: disable=too-many-nested-blocks
        for f in Path(p).glob("**/*.yml"):
            if f.is_file() and f.name not in exceptions:
                with open(f, "r", encoding="utf-8") as yml:
                    props = yaml.safe_load(yml)

                if f.name == "sources.yml":
                    for tbl in props["sources"][0]["tables"]:
                        for c in tbl["columns"]:
                            for dt in c.get("data_tests", {}):
                                test_names.append(dt[next(iter(dt.keys()))]["name"])
                else:
                    # obj lvl tests
                    for dt in props[o_type][0].get("data_tests", {}):
                        test_names.append(dt[next(iter(dt.keys()))]["name"])

                    # col lvl tests
                    for c in props[o_type][0]["columns"]:
                        for dt in c.get("data_tests", {}):
                            test_names.append(dt[next(iter(dt.keys()))]["name"])

    if len(test_names) != len(set(test_names)):
        dups = [test for test, cnt in Counter(test_names).items() if cnt > 1]
        errors.append(f"""Test names are not unique. Duplicates: {",".join(dups)}""")

    return errors


def main():
    """
    Runs all checks & throws error if any check failed
    """
    obj_paths = get_cfg()
    sources_path = "../dbt_analytics/models/stage/raw/sources.yml"

    errors = []

    errors.extend(check_obj_lvl_tests(obj_paths))
    errors.extend(check_sources_col_lvl_test_names(sources_path))
    errors.extend(check_mod_sed_snp_col_lvl_test_names(obj_paths))
    errors.extend(check_uniqueness_of_test_names(obj_paths))

    if errors:
        print("\n".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
