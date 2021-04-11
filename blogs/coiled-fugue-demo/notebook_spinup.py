import coiled

# Note that fugue requires Python 3.7 because of Spark compatibility
# Other dependencies are needed for parsing SQL
coiled.create_notebook(
    name="fugue-sql-demo",
    conda={"channels": ["conda-forge"], "dependencies": ["python=3.7.9", "dask", "gcc_linux-64", "ciso8601"]},
    pip=['fugue'],
    cpu=4,
    memory="16 GiB",
    files=["fugue-sql-demo.ipynb"],
    description="Analyzes dataset with Fugue",
)

# Create a software environment for our workers
coiled.create_software_environment(
    name="fugue-sql",
    conda={"channels": ["conda-forge"],
            "dependencies": ["dask=2021.03.1","distributed=2021.03.1","python=3.7.9","s3fs"]
    })