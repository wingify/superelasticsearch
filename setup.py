try:
    from setuptools import setup, find_packages
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

__name__ = 'superelasticsearch'
__version__ = '0.1.1'
__author__ = 'Wingify Engineering'
__author_email__ = 'dev@wingify.com'

long_description = '''superelasticsearch is utility library that extends a
particular version of elasticsearch library to provide some more utility
functions on top of it to make using Elasticsearch even easier.

SuperElasticsearch provides a few additional APIs that are sugar coated to
simplify using Elasticsearch in Python. These additional APIs are listed as
follows:

1. Iterated Search
2. Simple Bulk API

Read more with examples here: https://github.com/wingify/superelasticsearch
'''

setup(
    name=__name__,
    version=__version__,
    author=__author__,
    author_email=__author_email__,
    description=('An extended version of the official '
                 'Elasticsearch Python client.'),
    long_description=long_description,
    packages=find_packages(exclude=['tests']),
    install_requires = [
        'elasticsearch',
    ],
    include_package_data = True,
    zip_safe = False,
    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='nose.collector',
    tests_require=[
        'nose',
        'mock',
        'datadiff',
    ]
)
