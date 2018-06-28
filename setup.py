#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import division, print_function, unicode_literals, absolute_import

from setuptools import setup

setup(
    name='ldtable',
    py_modules=['ldtable'],
    long_description=open('readme.md').read(),
    version='20180628',
    description='in memory database like object with O(1) queries',
    url='https://github.com/Jwink3101/ldtable',
    author='Justin Winokur',
    author_email='Jwink3101@@users.noreply.github.com',
    license='MIT'
)
