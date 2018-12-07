from setuptools import find_packages, setup
#from numpy.distutils.core import setup, Extension
from json import load as json_load


if __name__ == '__main__':
    with open('setup.json', 'r') as info:
        kwargs = json_load(info)
    setup(
        include_package_data=True,
        packages=find_packages(),
        **kwargs
    )
