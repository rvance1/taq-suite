from pydantic import BaseModel, ConfigDict
import duckdb

from .taq_table import TaqTable

class Database(BaseModel):
    root_path: str | None = None
    conn: duckdb.DuckDBPyConnection | None = None

    # Allows pydantic to hold the duckdb connection object natively
    model_config = ConfigDict(arbitrary_types_allowed=True) 

    def connect(self, path: str) -> None:
        self.root_path = path
        self.conn = duckdb.connect()

        self.conn.execute(f"""
            CREATE OR REPLACE VIEW crsp_map AS 
            SELECT * FROM read_parquet('{self.root_path}/interim/crsp_mapping/*.parquet')
        """)
    
    def is_connected(self) -> bool:
        return self.root_path is not None and self.conn is not None

    def get_taq_table(self) -> TaqTable:
        if not self.is_connected():
            raise ValueError("Database is not connected")

        return TaqTable(root_dir=self.root_path + "/interim/taq", conn=self.conn)