from setuptools import setup

setup(
    install_requires=[
        'google-cloud-bigquery >= 2.30.1',
        'pytest >= 7.3.0',
    ],
    name='django-bigquery-exporter',
    packages=['bigquery_exporter'],
    version='0.1.0',
)
