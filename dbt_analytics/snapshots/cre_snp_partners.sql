--noqa: disable=all

{% snapshot cre_snp_partners %}
  {{config(
    target_schema='core',
    unique_key='product_name',
    strategy='timestamp',
    updated_at='updated_on'
  )}}

SELECT
    UPDATED_ON
    ,PRODUCT_NAME
    ,PARTNER_NAME
FROM
    {{ ref('stg_partners') }}

{% endsnapshot %}
