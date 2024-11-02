import os
import pyarrow.flight as pa_flight
from pyarrow import Table


# Get Env Variables
FLIGHT_HOST = os.getenv("SERVER_FLIGHT_HOST", "0.0.0.0")
FLIGHT_PORT = os.getenv("SERVER_FLIGHT_PORT", 8815)


def execute_query(sql_query: str) -> Table:
    # Connect to the Flight Server
    server_location = f"grpc://{FLIGHT_HOST}:{FLIGHT_PORT}"
    flight_client = pa_flight.connect(server_location)

    # Run query against the Flight Server
    flight_command_descriptor = pa_flight.FlightDescriptor.for_command(sql_query)  # TODO: Add SQL inject parser

    # Retrieve the Resulst Flight Descriptor
    response_flight = flight_client.get_flight_info(flight_command_descriptor)

    # Read content of the dataset
    reader = flight_client.do_get(response_flight.endpoints[0].ticket)
    result_table = reader.read_all()
    return response_flight, result_table
