import sys
from pathlib import Path

import yaml

ROOT_LEVEL_ORDER = ["version", "models"]
MODEL_PROPERTIES_ORDER = ["name", "description", "config", "data_tests", "columns"]
MODEL_CONFIG_ORDER = ["enabled", "schema", "materialized"]
COLUMNS_CONFIG_ORDER = ["name", "description", "data_type", "data_tests"]
OPTIONAL_COLUMNS_CONFIG = ["data_tests"]

try:
    file_path = Path(sys.argv[1])
except IndexError:
    print(
        "Error: No file path provided. Please provide the path to a YAML file to be validated"
    )
    sys.exit(1)

if not file_path.is_file():
    print(f"Error: The provided path '{file_path}' does not point to an existing YAML")
    sys.exit(1)

with file_path.open("r") as file:
    cfg = yaml.safe_load(file)

root_level_order = list(cfg.keys())
model_properties_order = list(cfg.get("models", [{}])[0].keys())
model_config_order = list(cfg.get("models", [{}])[0].get("config", {}).keys())
columns_config_orders = [
    list(col.keys()) for col in cfg.get("models", [{}])[0].get("columns", [])
]

if ROOT_LEVEL_ORDER == root_level_order:
    print("Root level correctly sorted")
else:
    print("Root level not correctly sorted")

if MODEL_PROPERTIES_ORDER == model_properties_order:
    print("Model properties correctly sorted")
else:
    print("Model properties not correctly sorted")

if MODEL_CONFIG_ORDER == model_config_order:
    print("Model configs correctly sorted")
else:
    print("Model configs not correctly sorted")

for i, col_order in enumerate(columns_config_orders):
    if COLUMNS_CONFIG_ORDER == col_order:
        print(f"Column config [{i}] correctly sorted")
    else:
        expected_without_optional = [
            item for item in COLUMNS_CONFIG_ORDER if item not in OPTIONAL_COLUMNS_CONFIG
        ]
        if expected_without_optional == col_order:
            print(
                f'Column config [{i}] correctly sorted (excluding optional property "data_tests")'
            )
        else:
            print(f"Column config [{i}] not correctly sorted")
