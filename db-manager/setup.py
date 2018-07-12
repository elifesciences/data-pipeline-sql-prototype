from setuptools import setup

import db_manager


DEPENDENCIES = (
    'psycopg2-binary',
    'natsort'
)


setup(
    name='db_manager',
    version=db_manager.__version__,
    description='',
    packages=['db_manager'],
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

