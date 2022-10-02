# Configure app.toml
# > snow login
# > snow configure
# Test locally
# > python app.py
# Create and deploy in snowflake:
# > snowcli function package
# > snowcli function create --environment dev
# > snowcli function execute -f 'helloFunction()'
import sys


def hello() -> str:
    return 'Hello World!'


# For local debugging. Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(hello(sys.argv[1:]))  # type: ignore
    else:
        print(hello()) # type: ignore
