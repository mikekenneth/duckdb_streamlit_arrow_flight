import pyarrow as pa
import pyarrow.flight as pa_flight

client = pa_flight.connect("grpc://0.0.0.0:8815")

# Upload a new dataset
data_table = pa.table([["Mario", "Luigi", "Peach"]], names=["Character"])
upload_descriptor = pa_flight.FlightDescriptor.for_path("uploaded.parquet")
writer, _ = client.do_put(upload_descriptor, data_table.schema)
writer.write_table(data_table)
writer.close()


# Retrieve metadata of newly uploaded dataset
flight = client.get_flight_info(upload_descriptor)
descriptor = flight.descriptor
print("Path:", descriptor.path[0].decode("utf-8"), "Rows:", flight.total_records, "Size:", flight.total_bytes)
print("=== Schema ===")
print(flight.schema)
print("==============")

# Read content of the dataset
reader = client.do_get(flight.endpoints[0].ticket)
read_table = reader.read_all()
print(read_table.to_pandas().head())

# List actions
print("==== List Actions====")
print(client.list_actions())

# Drop the newly uploaded dataset
client.do_action(pa_flight.Action("drop_dataset", "uploaded.parquet".encode("utf-8")))
