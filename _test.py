import sys
from pathlib import Path

import pytest
import yaml

ROOT_LEVEL_ORDER = ["version", "models"]
MODEL_PROPERTIES_ORDER = ["name", "description", "config", "data_tests", "columns"]
MODEL_CONFIG_ORDER = ["enabled", "schema", "materialized"]
COLUMNS_CONFIG_ORDER = ["name", "description", "data_type", "data_tests"]
OPTIONAL_COLUMNS_CONFIG = ["data_tests"]


def load_yaml(file_path):
    with file_path.open("r") as file:
        return yaml.safe_load(file)


def get_file_path(argv):
    if len(argv) < 2:
        raise ValueError(
            "Error: No file path provided. Please provide the path to a YAML file to be validated"
        )
    return Path(argv[1])


def validate_yaml_structure(cfg):
    root_level_order = list(cfg.keys())
    model_properties_order = list(cfg.get("models", [{}])[0].keys())
    model_config_order = list(cfg.get("models", [{}])[0].get("config", {}).keys())
    columns_config_orders = [
        list(col.keys()) for col in cfg.get("models", [{}])[0].get("columns", [])
    ]
    return (
        root_level_order,
        model_properties_order,
        model_config_order,
        columns_config_orders,
    )


def validate_order(expected, actual, message):
    assert expected == actual, f"{message}: expected {expected}, but got {actual}"


@pytest.fixture
def sample_yaml(tmp_path):
    content = """
    version: 1.0
    models:
      - name: model1
        description: Test model
        config:
          enabled: true
          schema: public
          materialized: table
        data_tests: []
        columns:
          - name: col1
            description: Test column
            data_type: string
            data_tests: []
    """
    file_path = tmp_path / "test.yaml"
    file_path.write_text(content)
    return file_path


def test_yaml_structure(sample_yaml):
    sys.argv = ["script_name", str(sample_yaml)]

    file_path = get_file_path(sys.argv)
    assert (
        file_path.is_file()
    ), f"Error: The provided path '{file_path}' does not point to an existing YAML file"

    cfg = load_yaml(file_path)

    (
        root_level_order,
        model_properties_order,
        model_config_order,
        columns_config_orders,
    ) = validate_yaml_structure(cfg)

    validate_order(
        ROOT_LEVEL_ORDER, root_level_order, "Root level order is not correct"
    )
    validate_order(
        MODEL_PROPERTIES_ORDER,
        model_properties_order,
        "Model properties order is not correct",
    )
    validate_order(
        MODEL_CONFIG_ORDER, model_config_order, "Model config order is not correct"
    )

    for i, col_order in enumerate(columns_config_orders):
        if col_order != COLUMNS_CONFIG_ORDER:
            expected_without_optional = [
                item
                for item in COLUMNS_CONFIG_ORDER
                if item not in OPTIONAL_COLUMNS_CONFIG
            ]
            validate_order(
                expected_without_optional,
                col_order,
                f"Column config [{i}] order is not correct",
            )


if __name__ == "__main__":
    pytest.main([__file__])
