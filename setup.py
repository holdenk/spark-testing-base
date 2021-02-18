from setuptools import setup

setup(
    name='sparktestingbase',
    version='0.0.7-snapshot',
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    packages=['sparktestingbase', 'sparktestingbase.test'],
    url='https://github.com/holdenk/spark-testing-base',
    license='LICENSE.txt',
    description='Spark testing for python',
    long_description='',
    install_requires=[
        'unittest2',
        'findspark',
        'pytest',
        'hypothesis==3.7.0'
    ],
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
)
