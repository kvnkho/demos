from datetime import timedelta, datetime
from prefect import task, Flow, Parameter
import prefect
import logging
from prefect.utilities.logging import get_logger
from pathlib import Path
import os

# os.mkdir('log')
LOG_PATH = 'log'

class MyFileLogger(logging.FileHandler):
    def __init__(self, filename, mode='a', encoding=None, delay=False):
        filename = os.path.join(LOG_PATH, filename)
        super(MyFileLogger, self).__init__(filename, mode, encoding, delay)

@task(max_retries=3, retry_delay=timedelta(seconds=1), name='pull_project', log_stdout=True)
def pull_project(project_name):
    logger = prefect.context.get("logger")
    logger.info(f"Pull latest {project_name} version from git.")
    logger.warning("A warning message....")
    return {'project_name': project_name}

def main():
    with Flow("test") as flow:
        project_info = pull_project(project_name='test')
    file_logger = get_logger()
    fh = MyFileLogger('auto_ai.log')
    fh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s | %(message)s"))
    file_logger.addHandler(fh)
    flow.run()

if __name__ == "__main__":
    main()