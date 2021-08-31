import prefect
from prefect import task, Flow
from prefect.tasks.snowflake import SnowflakeQuery, SnowflakeQueriesFromFile


query = """
    SHOW DATABASES;
"""

from pathlib import Path
p = Path("test_sql.sql")
p.write_text(query)

snowflake = SnowflakeQueriesFromFile(
    account="il66888.us-east-2.aws",
    user="kvnkho",
    password="password",
    file_path="test_sql.sql"
).run()

print(snowflake)