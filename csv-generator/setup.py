from setuptools import setup

import csv_generator


DEPENDENCIES = (
    'lxml==4.2.1',
    'pendulum',
    'tqdm',
    'requests'
)


setup(
    name='csv_generator',
    version=csv_generator.__version__,
    description='Utility for generating CSV data files from bespoke XML input files.',
    packages=['csv_generator',
              'csv_generator.consumers'],
    include_package_data=True,
    install_requires=DEPENDENCIES,
    license='MIT',
    url='https://github.com/elifesciences/data-pipeline.git',
    maintainer='eLife Sciences Publications Ltd.',
    maintainer_email='tech-team@elifesciences.org',
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.6",
    ]
)

