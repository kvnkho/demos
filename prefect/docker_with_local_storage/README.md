This is an example of how to run the setup for Docker RunConfig + Local Storage.

The steps inside are `environment.yml` are unrelated to the flow. They are there for demo purposes. The flow imports from the component directory.

The `setup.py` is to make this a module so that I can import the components. I was able to run my flow without installing the module in the Docker image...but this surprises me and I suggest adding a line in the Dockerfile to `pip install -e .` to install your module in the Docker image.

Steps

1. Build the Docker image with `docker build . -t test:latest`
2. Register the flow with `python workflow/flow.py`
3. Start your agent with `prefect agent docker start`
4. Run the flow with a `Quick Run` from the UI
