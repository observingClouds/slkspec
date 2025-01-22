import setuptools

# import versioneer

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="slkspec",
    #    version=versioneer.get_version(),
    #    cmdclass=versioneer.get_cmdclass(),
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
    python_requires=">=3.9",
    install_requires=[
        "fsspec>=0.9.0",
        "pyslk @ git+https://gitlab.dkrz.de/hsm-tools/pyslk.git@master",
    ],
    extras_require={
        "tests": [
            "zarr",
            "mypy",
            "black",
            "dask",
            "flake8",
            "mock",
            "netCDF4",
            "pandas",
            "pytest",
            "pytest-env",
            "pytest-cov",
            "testpath",
            "xarray",
        ],
        "preffs": [
            "fastparquet",
            "preffs @ git+https://github.com/d70-t/preffs.git@main",
            "aiohttp",
        ],
    },
    entry_points={
        "fsspec.specs": [
            "slk=slkspec.SLKFileSystem",
        ],
    },
)
