# Install setuptools if not installed.
try:
    import setuptools
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()

from setuptools import setup, find_packages


# read README as the long description
with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='autospec',
    version='0.1dev',
    description='Auto-specify UrbanSim models',
    long_description=long_description,
    author='UrbanSim Inc.',
    author_email='info@urbansim.com',
    url='https://github.com/urbansim/autospec',
    classifiers=[
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
    ],
    packages=find_packages(exclude=['*.tests']),
    install_requires=[
        'numpy >= 1.1.0',
        'pandas >= 0.16.0',
        'orca >= 1.3.0',
        'urbansim >= 0.1.1',
    ],
    extras_require={
        'pandana': ['pandana>=0.1']
    }
)
)