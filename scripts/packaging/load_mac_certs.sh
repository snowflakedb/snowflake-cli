#!/bin/bash -e -x

# Set the path to the temporary files for the certificates
APPLE_CERT_DEVELOPER_INSTALLER="apple_dev_installer_cert.p12"
APPLE_CERT_DEVELOPER_APPLICATION="apple_dev_application_cert.p12"

# Define the expected MD5 checksum
EXPECTED_INSTALLER_CHECKSUM="1f9d2dfd1a6dc87c87fe0426a6ee136e"
EXPECTED_APPLICATION_CHECKSUM="658613e0abe5c3187284e9662f18e1f0"

# Decode Developer ID Installer certificate from base64 into temporary file
base64 -d < $APPLE_CERT_DEVELOPER_INSTALLER_BASE64 > $APPLE_CERT_DEVELOPER_INSTALLER

# Calculate the actual checksum of the decoded file
ACTUAL_INSTALLER_CHECKSUM=$(md5 -q $APPLE_CERT_DEVELOPER_INSTALLER)

# Compare the actual checksum with the expected one
if [ "$ACTUAL_INSTALLER_CHECKSUM" == "$EXPECTED_INSTALLER_CHECKSUM" ]; then
  echo "$APPLE_CERT_DEVELOPER_INSTALLER: OK"
else
  echo "$APPLE_CERT_DEVELOPER_INSTALLER: FAILED"
  exit 1
fi

# Decode Developer ID Application certificate from base64 into temporary file
base64 -d < $APPLE_CERT_DEVELOPER_APPLICATION_BASE64 > $APPLE_CERT_DEVELOPER_APPLICATION

# Calculate the actual checksum of the decoded file
ACTUAL_APPLICATION_CHECKSUM=$(md5 -q $APPLE_CERT_DEVELOPER_APPLICATION)

# Compare the actual checksum with the expected one
if [ "$ACTUAL_APPLICATION_CHECKSUM" == "$EXPECTED_APPLICATION_CHECKSUM" ]; then
  echo "$APPLE_CERT_DEVELOPER_APPLICATION: OK"
else
  echo "$APPLE_CERT_DEVELOPER_APPLICATION: FAILED"
  exit 1
fi

# Set the Keychain path where the certificates will be imported
KEYCHAIN_PATH=$HOME/Library/Keychains/login.keychain-db

# Unlock the keychain using the provided password
security unlock-keychain -p $MAC_USERNAME_PASSWORD $KEYCHAIN_PATH

# Import Developer ID Installer certificate to the keychain
security import $APPLE_CERT_DEVELOPER_INSTALLER -k $KEYCHAIN_PATH -P $APPLE_CERT_DEVELOPER_INSTALLER_PASSWORD -T /usr/bin/codesign -T /usr/bin/security -T /usr/bin/xcrun -T /usr/bin/productsign -T /usr/bin/productbuild

# Check if the import was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to import installer certificate"
    exit 1
fi

# reload the keychain to ensure the changes are applied
security set-key-partition-list -S apple-tool:,apple: -k "$MAC_USERNAME_PASSWORD" $KEYCHAIN_PATH

# Import Developer ID Installer certificate to the keychain
security import $APPLE_CERT_DEVELOPER_APPLICATION -k $KEYCHAIN_PATH -P $APPLE_CERT_DEVELOPER_APPLICATION_PASSWORD -T /usr/bin/codesign -T /usr/bin/security -T /usr/bin/xcrun -T /usr/bin/productsign -T /usr/bin/productbuild

# Check if the import was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to import application certificate"
    exit 1
fi

# reload the keychain to ensure the changes are applied
security set-key-partition-list -S apple-tool:,apple: -k "$MAC_USERNAME_PASSWORD" $KEYCHAIN_PATH

# Inform the user about the successful import
echo "Certificates imported successfully to $KEYCHAIN_PATH"

# Clean up the temporary files
rm -f $APPLE_CERT_DEVELOPER_INSTALLER
rm -f $APPLE_CERT_DEVELOPER_APPLICATION

# Inform the user about the successful cleanup
echo "Temporary files cleaned up successfully"
