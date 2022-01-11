import urllib.request

import prefect
from prefect import task, unmapped, Flow
from prefect.executors import DaskExecutor, LocalExecutor
from prefect.run_configs import KubernetesRun, LocalRun
from prefect.storage import Local, Docker, GitHub

@task
def get_html(url):
    logger = prefect.context.get("logger")
    logger.info(f"Downloading {url}")
    with urllib.request.urlopen(url) as response:
        return (url,  response.read())

@task
def write_html(url_html):
    logger = prefect.context.get("logger")
    url, html = url_html
    basename = f"{url.split('://')[-1].strip('/').replace('/', '_')}.html"
    outname = f'/tmp/{basename}'
    logger.info(f"Writing {outname}")
    with open(outname, 'wb') as fh:
        fh.write(html)

with Flow('k8s test') as flow:
    urls = [
        'https://example.com',
        'https://docs.python.org/3/howto/urllib2.html'
    ]
    payload = get_html.map(urls)
    write_html.map(payload)

flow.run_config = KubernetesRun()
flow.storage = GitHub(repo="kvnkho/demos", path="prefect/k8s_test.py")
flow.register("bristech")
