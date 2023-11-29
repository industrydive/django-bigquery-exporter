from setuptools import setup

setup(
    install_requires=[
        'Django >= 3.2.4',
        'google-cloud-bigquery >= 2.30.1',
    ],
    name='django-bigquery-exporter',
    packages=['bigquery_exporter'],
    version='0.1.2',
)
