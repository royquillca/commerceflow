
from pathlib import Path


class SchemaLoader:
    def __init__(self, rds_client, cluster_arn, secret_arn, database):
        self.rds_client = rds_client
        self.cluster_arn = cluster_arn
        self.secret_arn = secret_arn
        self.database = database

    def execute_file(self, filepath):
        sql = Path(filepath).read_text(encoding="utf-8")

        # Supported for our simple schema.sql:
        # removes full-line comments and splits SQL at semicolons.
        lines = [
            line for line in sql.splitlines()
            if not line.strip().startswith("--")
        ]

        statements = [
            stmt.strip()
            for stmt in "\n".join(lines).split(";")
            if stmt.strip()
        ]

        for statement in statements:
            self.rds_client.execute_statement(
                resourceArn=self.cluster_arn,
                secretArn=self.secret_arn,
                database=self.database,
                sql=statement
            )

        return len(statements)
