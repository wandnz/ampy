from pkg_resources import Requirement, resource_filename

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

requires = [
    'psycopg2',
    'pylibmc'
    ]

setup(
    name='ampy',
    description='Python library for interacting with NNTSC data.',
    packages = find_packages(),
    install_requires=requires,
    version='2.1',
    author='Shane Alcock, Brendon Jones',
    author_email='contact@wand.net.nz',
    url='http://www.wand.net.nz',
)
