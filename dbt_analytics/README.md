# NOTE

This project does not strictly follow the dbt style guide in terms of cte usage ("import cte", "final cte").

For each seed, snapshot & model three files are created:
- doc file (markdown) e.g. __model.md
- schema file (yml) e.g. _model.yml
- model itself (sql) e.g. model.sql

It would also be possible to create a dedicated directory for each of these file types e.g. "docs" for doc files, preferably within each schema directory.
