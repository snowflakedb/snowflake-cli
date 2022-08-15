# Create and deploy in snowflake with:
# > zip -r app.zip .
# > snowcli function create -n helloFunction --handler 'app.hello' -f app.zip -i '' -r string

def hello() -> str:
    return 'Hello World!'