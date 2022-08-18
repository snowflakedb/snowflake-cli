from setuptools import setup, find_packages

setup(
    name='SnowCLI',
    version='0.1.0',
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'snowcli = snowcli:main',
        ],
    },
    packages=(
        find_packages() +
        find_packages(where="templates")
    ),
    package_data={'templates': ['*']},
    include_package_data=True
)
