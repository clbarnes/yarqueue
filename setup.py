#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages
import itertools

with open("README.rst") as readme_file:
    readme = readme_file.read()

requirements = ["redis>=3.2", "StrEnum>=0.4"]

extras_require = {
    "redislite": ["redislite>=5.0"],
    "pickle": ['pickle5; python_version >= "3.6" and python_version < "3.8"'],
    "cli": ["click", "tqdm"],
    "http": ["click", "flask"],
}

extras_require["all"] = list(
    set(itertools.chain.from_iterable(extras_require.values()))
)

setup_requirements = ["pytest-runner"]

test_requirements = ["pytest"]

setup(
    author="Chris L. Barnes",
    author_email="barnesc@janelia.hhmi.org",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
    ],
    description="Yet Another python queue backed by redis; but modern and compliant",
    install_requires=requirements,
    license="MIT license",
    long_description=readme,
    include_package_data=True,
    keywords="yarqueue queue multiprocessing hotqueue redis redislite",
    name="yarqueue",
    packages=find_packages(include=["yarqueue"]),
    package_data={"yarqueue": ["*.html"]},
    entry_points={
        "console_scripts": [
            "yarqwatch = yarqueue.watch.cli:yarqwatch",
            "yarqserve = yarqueue.watch.http:yarqserve",
        ]
    },
    setup_requires=setup_requirements,
    extras_require=extras_require,
    test_suite="tests",
    tests_require=test_requirements,
    url="https://github.com/clbarnes/yarqueue",
    version="0.3.0",
    zip_safe=False,
)
