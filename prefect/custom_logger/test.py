from prefect import task, Flow, Parameter
from prefect.engine.signals import FAIL, SKIP
from dataclasses import dataclass

@dataclass
class WriteConfig:
    target_base_path: str
    def validate(self):
        if not self.target_base_path:
            raise FAIL("'target_base_path' is required for hudi_options.")

@task
def load_configs(executor_instances, hudi_options: WriteConfig):
    hudiparms = WriteConfig(target_base_path=hudi_options[0])
    print(hudiparms)
	
	
with Flow('Write Table') as flow:
        executor_instances = Parameter('executor_instance', default=1)
        hudi_options= Parameter('hudi_options', default=["test_write"])
        load_configs(executor_instances, hudi_options) 

flow.register("dsdc")