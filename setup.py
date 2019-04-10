# Install recordio package by setup.py
# Local installation:
#     pip3 install .
# Make package:
#     python3 setup.py bdist_wheel
# Upload package:
#     twine upload dist/*

from setuptools import setup, find_packages

install_requires=['crc32c', 'python-snappy']

setup(name='pyrecordio',
      version='0.0.1',
      description='recordio file format support',
      url='https://github.com/ElasticDL/pyrecordio',
      author='Ant',
      author_email='XXX@antfin.com',
      license='TBD',
      packages=find_packages(),
      include_package_data=True,
      install_requires=install_requires)
