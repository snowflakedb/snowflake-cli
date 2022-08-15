# Test locally
# > python app.py
#
# Create and deploy in snowflake:
# > snowcli function build
# > snowcli function create -n helloFunction -h 'app.hello' -f app.zip -i '' -r string
# > snowcli function execute -f 'helloFunction()'
import sys


def hello() -> str:
    return 'Hello World!'


# For local debugging. Be aware you may need to type-convert arguments if you add input parameters
if __name__ == '__main__':
    if len(sys.argv) > 1:
        print(hello(sys.argv[1:]))
    else:
        print(hello())
