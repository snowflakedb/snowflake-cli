#!/bin/bash -e -x

# Set the Keychain path where the certificates will be imported
KEYCHAIN_PATH=$HOME/Library/Keychains/login.keychain-db

# Delete the Developer ID Installer certificate from the keychain
security delete-identity -c "Developer ID Installer: Snowflake Computing INC. (W4NT6CRQ7U)" "$KEYCHAIN_PATH"

# Delete the Developer ID Application certificate from the keychain
security delete-identity -c "Developer ID Application: Snowflake Computing INC. (W4NT6CRQ7U)" "$KEYCHAIN_PATH"

# Inform the user about the successful cleanup
echo "Temporary files cleaned up successfully"
