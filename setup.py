from setuptools import setup, find_packages

setup(
    install_requires=[
        'Django >= 3.2.4',
        'google-auth >= 1.32.1',
        'google-cloud-bigquery >= 2.30.1',
        'pytz >= 2019.1',
    ],
    name='django-bigquery-exporter',
    packages=find_packages(),
    version='0.2.3',
)
