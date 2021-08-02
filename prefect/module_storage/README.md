Demo for using Module storage with Prefect

1. Install `mymodule` with setup.py by doing `python setup.py bdist_wheel`. This will generate a wheel under the dist folder. `pip install name_of_wheel` to install the module.

2. Register the flows with `prefect register --project proj_name -p mymodule/flows/`. This should register 2 flows.

3. Make sure the agent as `mymodule` installed. You can then run flow1 or flow2 and it will import it from the module.ß