try:
    from setuptools.core import setup
except ImportError:
    from distutils.core import setup

setup(
    name='ampy',
    description='Python library for interacting with AMP data.',
    packages=['ampy'],
    version='0.0',
    author='',
    author_email='',
    url='',
    long_description=open('README.txt').read(),
)
