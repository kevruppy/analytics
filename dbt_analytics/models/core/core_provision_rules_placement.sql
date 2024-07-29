version: 2

models:
  - name: core_provision_rules
    description: Contains raw provision rules. There might be multiple rules per product
    config:
      enabled: True
      schema: core
      materialized: ephemeral
    data_tests:
      - row_count_source_match:
          source_name: raw
          table_name: provision_rules
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - PRODUCT_NAME
            - START_DATE
            - END_DATE
          config:
            severity: warn
    columns:
      - name: _CHECK_SUM
        description: Hashed concatenation of product, start and end to remove duplicates downstream
        data_type: VARCHAR
      - name: PRODUCT_NAME
        description: Name of the product to which rule applies
        data_type: VARCHAR
        data_tests:
          - not_null
          - dbt_utils.not_empty_string
      - name: START_DATE
        description: Date since when rule is valid
        data_type: DATE
        data_tests:
          - not_null
          - dbt_utils.accepted_range:
              max_value: "CURRENT_DATE()"
      - name: END_DATE
        description: Date until rule is valid
        data_type: DATE
        data_tests:
          - not_null
          - dbt_utils.accepted_range:
              max_value: "CURRENT_DATE()"
          - dbt_utils.expression_is_true:
              expression: "> START_DATE"
      - name: BASE_PROVISION
        description: Base provision according to this rule
        data_type: FLOAT
        data_tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
      - name: PLACEMENT_PROVISION
        description: Placement provision according to this rule (can be null)
        data_type: FLOAT
      - name: PROPORTIONAL_PROVISION
        description: Proportional provision according to this rule (can be null)
        data_type: JSON
