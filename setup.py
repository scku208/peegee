import os
from distutils.core import setup

VERSION = '0.2.1'


setup(
    name = 'peegee',
    py_modules = ['peegee'],
    license = 'MIT',
    version = VERSION,
    description = 'A PostgreSQL client based on psycopg2',
    install_requires=['psycopg2',],
    author = 'scku',
    author_email = 'scku208@gmail.com',
    url = 'https://github.com/scku208/peegee',
    download_url = 'https://github.com/scku208/peegee/archive/{v}.tar.gz'\
        .format(v=VERSION),
    keywords = ['postgresql', 'psycopg2', 'database', 'peegee'],
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        ]
    )
