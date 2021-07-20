from dremio_connection import connect_to_dremio_flight_server_endpoint

query = """
    SELECT * FROM "Demo NYC Taxi"."Demo Taxi Reflection" LIMIT 10
"""

df = connect_to_dremio_flight_server_endpoint(hostname="3.238.152.255", flightport=32010, username="kvnkho",
    password="Prefect123", sqlquery=query, tls=None, certs=None)

print(df.head())