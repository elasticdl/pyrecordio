# Install recordio package by setup.py
# Local installation:
#     pip3 install .
# Make package:
#     python3 setup.py bdist

from setuptools import setup, find_packages

packages = find_packages(where='.', exclude=['*.pyc'])

install_requires=['crc32c']

setup(name='recordio',
      version='0.0.1',
      description='recordio file format support',
      url='https://github.com/ElasticDL/pyrecordio',
      author='Ant',
      author_email='XXX@antfin.com',
      license='TBD',
      packages=packages,
      include_package_data=True,
      install_requires=install_requires)
