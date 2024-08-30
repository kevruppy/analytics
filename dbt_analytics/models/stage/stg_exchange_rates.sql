SELECT
	_ID
	,(LOAD_RESULT ->> 'date')::DATE AS VALID_FROM
	,(LOAD_RESULT ->> 'date')::DATE AS VALID_TO
	,(LOAD_RESULT ->> 'rates' ->> 'USD')::FLOAT AS EXCHANGE_RATE
FROM
	{{ source('raw', 'exchange_rates') }}
ORDER BY
	_ID
