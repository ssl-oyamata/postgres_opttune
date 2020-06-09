import os
import setuptools
from setuptools import setup


def read_requirements():
    """Parse requirements from requirements.txt."""
    reqs_path = os.path.join('.', 'requirements.txt')
    with open(reqs_path, 'r') as f:
        requirements = [line.rstrip() for line in f]
    return requirements


setup(
    name="pgopttune",
    version="0.11",
    description='Trying PostgreSQL parameter tuning using machine learning',
    author="postgres-opttune development team",
    url='https://github.com/ssl-oyamata/postgres_opttune',
    license='Apache License 2.0',
    packages=setuptools.find_packages(),
)
