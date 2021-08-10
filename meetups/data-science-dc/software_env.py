import coiled

# Create a software environment for our workers
coiled.create_software_environment(
    name="prefect2",
    conda={"channels": ["conda-forge"],
            "dependencies": ["python=3.8.0", "dask=2021.05.0", "distributed=2021.05.0", "pandas", "prefect", "seaborn", "boto3"]},
    )