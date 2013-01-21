try:
    from setuptools.core import setup
except ImportError:
    from distutils.core import setup

requires = [
    'sqlalchemy'
    ]

setup(
    name='ampy',
    description='Python library for interacting with AMP data.',
    packages=['ampy'],
    install_requires=requires,
    version='0.0',
    author='',
    author_email='',
    url='',
    long_description=open('README.txt').read(),
)
