from setuptools import find_packages, setup

setup(
    name="{_project_folder_}",
    packages=find_packages(),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={{"dev": ["dagster-webserver", "pytest"]}},
)
