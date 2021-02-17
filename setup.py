from setuptools import setup

setup(
    name='sparktestingbase',
    version='1.0.1',
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
        'pytest'
    ],
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
)
