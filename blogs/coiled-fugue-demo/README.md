# Coiled fugue-sql demo

This is a demo that shows how to use fugue-sql to run SQL queries on top of Dask DataFrames through (Coiled)[https://coiled.io/]. fugue aims to be compatible between Pandas, Spark, and Dask, which means that Python version 3.7.9 is needed.

There is a requirements file to install the dependencies. There is a problem where the Dask workers will die if the versions of libraries between the Client, Scheduler, and Workers do not match.