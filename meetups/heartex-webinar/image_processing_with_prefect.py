import boto3
import matplotlib.image as mpimg
import numpy as np
from io import BytesIO
from PIL import Image, ImageEnhance
from typing import List

from prefect import Flow, task, Parameter
import prefect 
from prefect.executors import DaskExecutor
from prefect.tasks.notifications import SlackTask
from prefect.tasks.control_flow.case import case

@task
def get_images() -> List[str]:
    s3 = boto3.resource('s3', region_name='us-east-2')
    bucket = s3.Bucket('label-studio-raw-images')
    images = []
    for bucket_object in bucket.objects.all():
        images.append(bucket_object.key)
    return images

@task
def load_image(image_name):
    s3 = boto3.resource('s3', region_name='us-east-2')
    bucket = s3.Bucket('label-studio-raw-images')
    image_object = bucket.Object(image_name)
    image = mpimg.imread(BytesIO(image_object.get()['Body'].read()), 'png')
    return image

@task
def convert_to_pil(image):
    return Image.fromarray((image * 255).astype(np.uint8))

@task
def improve_contrast(image, factor = 2):
    #image brightness enhancer
    enhancer = ImageEnhance.Contrast(image)
    return enhancer.enhance(factor)

@task
def resize(image, size=256):
    return image.resize((size, size))

@task
def save_image(image, image_name):
    logger = prefect.context.get("logger")
    logger.info("Processing image " + image_name)

    buffer = BytesIO()
    image.save(buffer, "png")
    buffer.seek(0)

    s3 = boto3.resource('s3', region_name='us-east-2')
    processed_bucket = s3.Bucket('label-studio-processed-images')
    processed_bucket.put_object(Key=image_name, Body=buffer)
    return

post_to_slack = SlackTask(
          message="The images have been processed",
          webhook_secret="SLACK_WEBHOOK_URL")


with Flow('image-dask') as flow:
    size = Parameter("size", default=256)
    image_files = get_images()
    images = load_image.map(image_files)
    images = convert_to_pil.map(images)
    images = improve_contrast.map(images)
    images = resize.map(images, size)
    saving = save_image.map(images, image_files)
    post_to_slack(upstream_tasks=[saving])

import coiled

coiled.create_software_environment(
   name="prefect",
   conda={"channels": ["conda-forge"],
           "dependencies": ["python=3.8.0", "prefect"]},
   pip=["boto3", "matplotlib"]
   )

executor = DaskExecutor(
   cluster_class=coiled.Cluster,
   cluster_kwargs={
       "software": "kvnkho/prefect",
       "shutdown_on_close": True,
       "name": "prefect-cluster",
   },
)

from prefect.run_configs import LocalRun
flow.run_config = LocalRun(env={"PREFECT__CLOUD__AGENT__LEVEL":"DEBUG"})
flow.executor = executor
flow.register('heartex')