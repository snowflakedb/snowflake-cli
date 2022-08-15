# Test locally
# > python app.py
# 
# Create and deploy in snowflake:
# > snowcli function build
# > snowcli function create -n helloFunction --handler 'app.hello' -f app.zip -i '' -r string
# > snowcli function execute -f 'helloFunction()'

def hello() -> str:
    return 'Hello World!'

if __name__ == '__main__':
    print(hello())