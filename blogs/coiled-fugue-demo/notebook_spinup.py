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