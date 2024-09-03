-- sqlfluff:rules:references.keywords:ignore_words:COLUMNS

SELECT COUNT(*) = 0 AS _CHECK_PASSED
FROM ANALYTICS.INFORMATION_SCHEMA.COLUMNS
WHERE
	TABLE_SCHEMA NOT IN ({{ var('ignored_schemas') }})
	AND COLUMN_COMMENT IS NULL
