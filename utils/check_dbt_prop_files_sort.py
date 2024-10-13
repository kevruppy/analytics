# pylint: disable=duplicate-code,too-many-branches,too-many-nested-blocks

import json
import sys
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


def check_dbt_analyses_order(path_to_analyses_yml: str) -> List[str]:
    """
    Validates order of properties for dbt analyses
    Order/sort convention:
        - Property `version` needs to be mentioned first
            - Within property `analyses` order convention is: `name`, `description`, `docs`
                - Within property `docs` the property `show` needs to be mentioned first

    Params:
        path_to_analyses_yml (str): Path to dbt analyses properties yml

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []

    with open(path_to_analyses_yml, "r", encoding="utf-8") as yml:
        props = yaml.safe_load(yml)

    if list(props.keys()) != ["version", "analyses"]:
        errors.append("Prop `version` not mentioned first in `analyses`")

    for p in props["analyses"]:
        if list(p.keys()) != ["name", "description", "docs"]:
            errors.append("Props of `analyses` violate sort conv")

        if list(p["docs"].keys())[0] != "show":
            errors.append("Prop `docs` of `analyses` does not use `show` as first prop")

    return errors


def check_dbt_macros_order(path_to_macros_yml_files: str) -> List[str]:
    """
    Validates order of properties for dbt macros
    Order/sort convention:
        - Property `version` needs to be mentioned first
            - Within property `macros` order convention is: `name`, `description`
                - If macro uses args order convention is: `name`, `description`, `arguments`

    Params:
        path_to_macros_yml_files (str): Path to dbt macros properties yml files

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []

    for f in Path(path_to_macros_yml_files).glob("*.yml"):
        with open(f, "r", encoding="utf-8") as yml:
            props = yaml.safe_load(yml)

        m_n = props["macros"][0]["name"]

        if list(props.keys()) != ["version", "macros"]:
            errors.append(f"Props of macros obj `{m_n}` do not start with `version`")

        if "arguments" in list(props["macros"][0].keys()):
            exp_list = ["name", "description", "arguments"]
        else:
            exp_list = ["name", "description"]

        if list(props["macros"][0].keys()) != exp_list:
            errors.append(f"Props of macros obj `{m_n}` violates sort conv")

    return errors


def check_dbt_mod_sed_snp_order(path_to_dbt_objects: str) -> List[str]:
    """
    Validates order of properties for dbt models, seeds & snapshots
    Order/sort convention:
        - Property `version` needs to be mentioned first
            - Within property {obj_type} order convention is:
                --> `name`, `description`, `config`, [`data_tests`], `columns`
                - For property `config` order convention depends on obj_type:
                    models: Only first three configs are checked:
                        --> `enabled`, `schema`, `materialized`
                    seeds:
                        --> `enabled`, `delimiter`, `schema`, `column_types`
                    snapshots:
                        --> Not checked (handled via SQL files)


    Params:
        path_to_dbt_objects (str): Path to dbt objects properties yml

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []

    for o_t, paths in path_to_dbt_objects.items():
        for p in paths:
            for f in Path(p).glob("*.yml"):
                with open(f, "r", encoding="utf-8") as yml:
                    props = yaml.safe_load(yml)

                o_n = props[o_t][0]["name"]

                if list(props.keys()) != ["version", o_t]:
                    errors.append(f"Props of {o_t} obj `{o_n}` dont start with `version`")

                # if obj lvl tests are used they need to be mentioned before columns
                for p in props[o_t]:
                    if "data_tests" in list(p.keys()):
                        exp_list = ["name", "description", "config", "data_tests", "columns"]
                    else:
                        exp_list = ["name", "description", "config", "columns"]

                    if list(p.keys()) != exp_list:
                        errors.append(f"Props of {o_t} obj `{o_n}` violates sort conv")

                # for models we only check order of first three configs
                if o_t == "models":
                    if list(props[o_t][0]["config"].keys())[0:3] != [
                        "enabled",
                        "schema",
                        "materialized",
                    ]:
                        errors.append(f"Props of models obj `{o_n}` violates sort conv")

                # check seeds config order
                if o_t == "seeds":
                    if list(props["seeds"][0]["config"].keys()) != [
                        "enabled",
                        "delimiter",
                        "schema",
                        "column_types",
                    ]:
                        errors.append(f"Props of seeds obj `{o_n}` violates sort conv")

                # check order of obj lvl tests config
                for dt in props[o_t][0].get("data_tests", {}):
                    for _, v in dt.items():
                        if list(v.keys())[0] != "name":
                            errors.append(
                                f"Props of obj lvl test for {o_t} obj `{o_n}` violates sort conv"
                            )

                # check cols config
                for c in props[o_t][0]["columns"]:
                    c_keys = list(c.keys())
                    if o_t == "seeds":
                        if c_keys != ["name", "description", "data_tests"]:
                            errors.append(f"Props of cols in seeds obj `{o_n}` violate sort conv")
                    else:
                        if len(c_keys) == 3:
                            if c_keys != ["name", "description", "data_type"]:
                                errors.append(
                                    f"Props of cols in {o_t} obj `{o_n}` violate sort conv"
                                )
                        elif len(c_keys) == 4:
                            if c_keys != ["name", "description", "data_type", "data_tests"]:
                                errors.append(
                                    f"Props of cols in {o_t} obj `{o_n}` violate sort conv"
                                )
                        else:
                            if c_keys != [
                                "name",
                                "description",
                                "data_type",
                                "constraints",
                                "data_tests",
                            ]:
                                errors.append(
                                    f"Props of cols in {o_t} obj `{o_n}` violate sort conv"
                                )

                    # check order of col lvl tests config
                    for dt in c.get("data_tests", {}):
                        if next(iter(list(dt.values())[0].keys())) != "name":
                            errors.append(
                                f"Props of col lvl tests in {o_t} obj `{o_n}` violate sort conv"
                            )

    return errors


def check_dbt_sources_order(path_to_sources_yml_file) -> List[str]:
    """
    Validates order of properties for dbt sources
    Order/sort convention:
        - Property `version` needs to be mentioned first

    Params:
        path_to_analyses_yml (str): Path to dbt sources properties yml

    Returns:
        errors (list[str]): List of errors (empty if no errors found)
    """
    errors = []

    with open(path_to_sources_yml_file, "r", encoding="utf-8") as yml:
        props = yaml.safe_load(yml)

    if list(props.keys()) != ["version", "sources"]:
        errors.append("Prop `version` not mentioned first in `sources`")

    # check order of sources config
    for s in props["sources"]:
        if list(s.keys()) != [
            "name",
            "schema",
            "description",
            "loader",
            "loaded_at_field",
            "tags",
            "tables",
        ]:
            errors.append("Props of `sources` violate sort conv")

    # check order tables config
    for s in props["sources"]:
        for t in s["tables"]:
            if list(t.keys()) != ["name", "description", "columns"]:
                errors.append(f"""Props for tbl `{t["name"]}` in `sources` violate sort conv""")

    # check order of cols config
    for s in props["sources"]:
        for t in s["tables"]:
            for c in t["columns"]:
                if list(c.keys()) != ["name", "description", "data_type", "data_tests"]:
                    errors.append(
                        f"""Props for cols of tbl {t["name"]} in `sources` violate sort conv"""
                    )

    return errors


def main():
    """
    Runs all checks & throws error if any check failed
    """
    obj_paths = get_cfg()

    errors = []

    errors.extend(check_dbt_analyses_order(obj_paths["analyses"]))
    errors.extend(check_dbt_macros_order(obj_paths["macros"]))
    errors.extend(
        check_dbt_mod_sed_snp_order({k: obj_paths[k] for k in ["models", "seeds", "snapshots"]})
    )
    errors.extend(check_dbt_sources_order(obj_paths["sources"]))

    if errors:
        print("\n".join(errors))
        sys.exit(1)


if __name__ == "__main__":
    main()
