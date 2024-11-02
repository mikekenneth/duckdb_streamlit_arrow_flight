import os
import duckdb
import pyarrow.parquet as pa_parquet
import pyarrow.flight as pa_flight
from pyarrow import Table as pa_table
from uuid import uuid4
from pathlib import Path


class DuckDBClient:
    def __init__(self, db_location: Path = None):
        if db_location is None:
            self.db_location = Path("data/duck.db")
        else:
            self.db_location = db_location
        self.db_location.parent.mkdir(exist_ok=True)
        self.duckdb_client = duckdb.connect(self.db_location)

    def query(self, sql_query) -> pa_table:
        result = self.duckdb_client.query(sql_query)
        return result.arrow()


class FlightServer(pa_flight.FlightServerBase):

    def __init__(self, location="grpc://0.0.0.0:8815", repo: Path = Path("./datasets"), **kwargs):
        super(FlightServer, self).__init__(location, **kwargs)
        self._location = location
        self._repo = repo

    def _duckdb_query_handler(self, sql: str) -> str:
        duck_client = DuckDBClient()
        return duck_client.query(sql_query=sql)

    def _make_flight_info(self, dataset):
        dataset_path = self._repo / dataset
        schema = pa_parquet.read_schema(dataset_path)
        metadata = pa_parquet.read_metadata(dataset_path)
        descriptor = pa_flight.FlightDescriptor.for_path(dataset.encode("utf-8"))
        endpoints = [pa_flight.FlightEndpoint(dataset, [self._location])]
        return pa_flight.FlightInfo(schema, descriptor, endpoints, metadata.num_rows, metadata.serialized_size)

    def list_flights(self, context, criteria):
        for dataset in self._repo.iterdir():
            yield self._make_flight_info(dataset.name)

    def get_flight_info(self, context, descriptor: pa_flight):
        print(self._repo)
        if descriptor.descriptor_type == pa_flight.DescriptorType.CMD:
            command = descriptor.command.decode("utf-8")
            print(f"Executing the SQL command: {command}")
            query_result_arrow_table = self._duckdb_query_handler(sql=command)
            dataset_name = f"{str(uuid4())}.parquet"
            pa_parquet.write_table(query_result_arrow_table, self._repo / dataset_name)
            print(f"Result Dataset: {dataset_name}")
            return self._make_flight_info(dataset=dataset_name)
        return self._make_flight_info(descriptor.path[0].decode("utf-8"))

    # TODO: You could use do_put to allow users to upload temporal tables
    def do_put(self, context, descriptor, reader, writer):
        dataset = descriptor.path[0].decode("utf-8")
        dataset_path = self._repo / dataset
        data_table = reader.read_all()
        pa_parquet.write_table(data_table, dataset_path)

    def do_get(self, context, ticket):
        dataset = ticket.ticket.decode("utf-8")
        dataset_path = self._repo / dataset
        return pa_flight.RecordBatchStream(pa_parquet.read_table(dataset_path))

    def list_actions(self, context):
        return [("drop_dataset", "Delete a dataset.")]

    # TODO: Decide if we keep this or not
    def do_action(self, context, action):
        if action.type == "drop_dataset":
            self.do_drop_dataset(action.body.to_pybytes().decode("utf-8"))
        elif action.type == "query_sql":
            print("SQL Query Start")
            sql_query = action.body.to_pybytes().decode("utf-8")
            print(sql_query)
            print("SQL Query END")
            return sql_query.encode()
        else:
            raise NotImplementedError

    def do_drop_dataset(self, dataset):
        dataset_path = self._repo / dataset
        dataset_path.unlink()


if __name__ == "__main__":
    # Get Env Variables
    FLIGHT_HOST = os.getenv("SERVER_FLIGHT_HOST", "0.0.0.0")
    FLIGHT_PORT = os.getenv("SERVER_FLIGHT_PORT", 8815)
    FLIGHT_DATA_DIR_TYPE = os.getenv("SERVER_FLIGHT_DATA_DIR_TYPE", "local")
    FLIGHT_DATA_DIR = os.getenv("SERVER_FLIGHT_DATA_DIR_BASE", "./datasets")

    print("Initiating Flight Server")
    server_location = f"grpc://{FLIGHT_HOST}:{FLIGHT_PORT}"
    server_data_dir = Path(FLIGHT_DATA_DIR)
    server_data_dir.mkdir(exist_ok=True)
    server = FlightServer(location=server_location, repo=server_data_dir)

    print("Starting Flight Server !")
    server.serve()
