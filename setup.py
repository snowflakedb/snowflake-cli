from setuptools import setup

setup(
    name='SnowCLI',
    version='0.1.0',
    py_modules=['snowcli'],
    install_requires=[
        'Click',
    ],
    entry_points={
        'console_scripts': [
            'snowcli = snowcli:cli',
        ],
    },
)