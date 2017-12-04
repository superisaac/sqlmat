from distutils.core import setup
from setuptools import find_packages

setup(name='sqlmat',
      version='0.0.1',
      description='simply map python3 statement to postgresql statement',
      author='Zeng Ke',
      author_email='superisaac.ke@gmail.com',
      packages=find_packages(),
      install_requires=[
          'asyncpg'
      ]
)

