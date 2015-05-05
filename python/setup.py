from setuptools import setup

setup(
    name='sparktestingbase',
    version='0.0.7-snapshot',
    author='Holden Karau',
    author_email='holden@pigscanfly.ca',
    packages=['spark-testing-base', 'spark-testing-base.test'],
    url='https://github.com/holdenk/spark-testing-base',
    license='LICENSE.txt',
    description='Spark testing for python',
    long_description=open('../README.md').read(),
    install_requires=[
        'unittest2'
    ],
    test_requires=[
        'nose',
        'coverage',
        'unittest2'
    ],
)
