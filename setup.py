import setuptools

import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt") as f:
    requirements = f.read().strip().split("\n")

setuptools.setup(
    name="slkspec",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Hauke Schulz",
    author_email="haschulz@uw.edu",
    description="fsspec implementation for StrongLink tape archive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/observingClouds/slkspec",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "fsspec>=0.9.0",
        # "pyslk @ git+https://gitlab.dkrz.de/hsm-tools/pyslk.git@master"
    ],
    entry_points={
        "fsspec.specs": [
            "slk=slkspec.SLKFileSystem",
        ],
    },
)
