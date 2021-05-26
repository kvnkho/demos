import boto3
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
from io import BytesIO
from PIL import Image, ImageEnhance
from typing import List


def get_images() -> List[str]:
    s3 = boto3.resource('s3', region_name='us-east-2')
    bucket = s3.Bucket('label-studio-raw-images')
    images = []
    for bucket_object in bucket.objects.all():
        images.append(bucket_object.key)
    return images

def load_image(image_name):
    s3 = boto3.resource('s3', region_name='us-east-2')
    bucket = s3.Bucket('label-studio-raw-images')
    image_object = bucket.Object(image_name)
    image = mpimg.imread(BytesIO(image_object.get()['Body'].read()), 'png')
    return image

def convert_to_pil(image):
    return Image.fromarray((image * 255).astype(np.uint8))

def improve_contrast(image, factor = 2):
    #image brightness enhancer
    enhancer = ImageEnhance.Contrast(image)
    return enhancer.enhance(factor)

def resize(image, size=256):
    return image.resize((size, size))

def save_image(image, image_name):
    buffer = BytesIO()
    image.save(buffer, "png")
    buffer.seek(0)

    s3 = boto3.resource('s3', region_name='us-east-2')
    processed_bucket = s3.Bucket('label-studio-processed-images')
    processed_bucket.put_object(Key=image_name, Body=buffer)
    return

def post_to_slack():
    pass

images = get_images()
for image_file in images:
    print("Processing " + image_file)
    image = load_image(image_file)
    image = convert_to_pil(image)
    image = improve_contrast(image)
    image = resize(image)
    save_image(image, image_file)
    post_to_slack()