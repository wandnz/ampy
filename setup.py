#
# This file is part of ampy.
#
# Copyright (C) 2013-2017 The University of Waikato, Hamilton, New Zealand.
#
# Authors: Shane Alcock
#          Brendon Jones
#
# All rights reserved.
#
# This code has been developed by the WAND Network Research Group at the
# University of Waikato. For further information please see
# http://www.wand.net.nz/
#
# ampy is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.
#
# ampy is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with ampy; if not, write to the Free Software Foundation, Inc.
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
#
# Please report any bugs, questions or comments to contact@wand.net.nz
#

from pkg_resources import resource_filename

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup

requires = [
    'psycopg2>=2.5.4',
    'pylibmc>=1.2.2',
    'bcrypt>=2.0.0',
    ]

setup(
    name='ampy',
    description='Python library for interacting with NNTSC data.',
    packages = find_packages(),
    install_requires=requires,
    version='2.15',
    author='Shane Alcock, Brendon Jones',
    author_email='contact@wand.net.nz',
    url='http://www.wand.net.nz',
)
