from setuptools import setup, find_packages

setup(
    name='variable_generators',
    version='0.1',
    description='Bulk definition of explanatory variables for UrbanSim',
    long_description=
        'Functions that facilitate bulk creation of Orca column functions for use as '
        'explanatory variables in UrbanSim.',
    author='UrbanSim Inc.',
    author_email='info@urbansim.com',
    url='https://github.com/udst/variable_generators',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'numpy >= 1.1',
        'pandas >= 0.16',
        'orca >= 1.3',
        'urbansim >= 0.1.1',
    ],
    extras_require={
        'pandana': ['pandana>=0.1']
    }
)
