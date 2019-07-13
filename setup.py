import sys
import os.path

from setuptools import find_packages
from distutils.core import setup
from os import listdir

# Fetch the requirements from the requirements file.
requirements = []
with open("requirements.txt", 'r') as f:
    requirements = [l.strip() for l in f.readlines()]

print (find_packages())
setup(name="ruciopylib",
    version='1.0.0-alpha.1',
    packages=find_packages(exclude=['tests']),
    scripts=[],
    description="Python library to run the rucio client locally",
    long_description='Allows interaction with the rucio command line, grabbing data from its output, downloading files, etc.',
    author="G. Watts (IRIS-HEP)",
    author_email="gwatts@uw.edu",
    maintainer="Gordon Watts (IRIS-HEP)",
    maintainer_email="gwatts@uw.edu",
    url="http://iris-hep.org",
    download_url="http://iris-hep.org",
    license="TBD",
    test_suite="tests",
    install_requires=requirements,
    setup_requires=["pytest-runner"],
    tests_require=["pytest>=3.9"],
    classifiers=[
        # "Development Status :: 1 - Planning",
        "Development Status :: 2 - Pre-Alpha",
        # "Development Status :: 3 - Alpha",
        # "Development Status :: 4 - Beta",
        # "Development Status :: 5 - Production/Stable",
        # "Development Status :: 6 - Mature",
        "Intended Audience :: Developers",
        "Intended Audience :: Information Technology",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.7",
        "Topic :: Software Development",
        "Topic :: Utilities",
    ],
    data_files=[],
    platforms="Any",
)
