try:
    from setuptools import setup, find_packages
except ImportError:
    from distribute_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

__name__ = 'superelasticsearch'
__version__ = '0.1.0'
__author__ = 'Wingify Engineering'
__author_email__ = 'dev@wingify.com'

# read contents of a file
read_file = lambda x: open(x, 'r').read()

setup(
    name=__name__,
    version=__version__,
    author=__author__,
    author_email=__author_email__,
    description=('An extended version of the official '
                 'Elasticsearch Python client.'),
    long_description=read_file('README.md'),
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
