import yaml
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self):
        self.creds = self.read_db_creds()
        self.engine = self.init_db_engine()

    def read_db_creds(self, creds_file='db_creds.yaml'):
        with open(creds_file, 'r') as file:
            return yaml.safe_load(file)

    def init_db_engine(self):
        creds = self.creds
        engine = create_engine(f"postgresql://{creds['RDS_USER']}:{creds['RDS_PASSWORD']}@{creds['RDS_HOST']}:{creds['RDS_PORT']}/{creds['RDS_DATABASE']}")
        return engine

    def list_db_tables(self):
        return self.engine.table_names()

    def upload_to_db(self, data, table_name):
        data.to_sql(table_name, self.engine, index=False, if_exists='replace')
