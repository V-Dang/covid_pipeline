class PipelineConfig():
    def __init__(self, api_url: str, s3_prefix: str, postgres_table: str):
        """Pipeline configuration object. Parameters for the Pipeline Class components.

        Args:
            api_url (str): URL endpoint for API.
            s3_prefix (str): Subfolder prefix in S3 bucket (table).
            postgres_table (str): Postgres table name.
        """
        self.api_url = api_url
        self.s3_prefix = s3_prefix
        self.postgres_table = postgres_table
