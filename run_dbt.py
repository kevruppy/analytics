import os
from dbt.cli.main import dbtRunner, dbtRunnerResult

os.chdir("/workspaces/analytics/dbt_analytics")

dbt = dbtRunner()

res: dbtRunnerResult = dbt.invoke(["clean"])

print(res)

res: dbtRunnerResult = dbt.invoke(["build"])
