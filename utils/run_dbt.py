import os
from pathlib import Path

from dbt.cli.main import dbtRunner, dbtRunnerResult

os.chdir(Path(__file__).parents[1] / "dbt_analytics")

dbt = dbtRunner()

res: dbtRunnerResult = dbt.invoke(["clean"])

res: dbtRunnerResult = dbt.invoke(["build"])

# generate `catalog.json`

# dbt.invoke(["docs", "generate"])

# show docs

# dbt.invoke(["docs", "serve"])
