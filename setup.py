from distutils.core import setup
setup(
    name = 'peegee',
    packages = ['peegee'],
    version = '0.1.0',
    description = 'A pythonic PostgreSQL client based on psycopg2 suit only for Python3',
    install_requires=['psycopg2',],
    author = 'scku',
    author_email = 'scku208@gmail.com',
    url = 'https://github.com/scku208/peegee',
    download_url = 'https://github.com/scku208/peegee/archive/0.1.0.tar.gz',
    keywords = ['postgresql', 'psycopg2', 'database'],
    classifiers = [],
)