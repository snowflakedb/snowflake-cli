#!/bin/bash -e -x

# Set the path to the temporary files for the certificates
APPLE_CERT_DEVELOPER_INSTALLER="apple_dev_installer_cert.p12"
APPLE_CERT_DEVELOPER_APPLICATION="apple_dev_application_cert.p12"

# Decode Developer ID Installer certificate from base64 into temporary file
base64 -d $APPLE_CERT_DEVELOPER_INSTALLER_BASE64 > $APPLE_CERT_DEVELOPER_INSTALLER

# Check the checksum of the decoded Developer ID Installer certificate
echo "1f9d2dfd1a6dc87c87fe0426a6ee136e $APPLE_CERT_DEVELOPER_INSTALLER" | md5sum -c -

# Decode Developer ID Application certificate from base64 into temporary file
base64 -d $APPLE_CERT_DEVELOPER_APPLICATION_BASE64 > $APPLE_CERT_DEVELOPER_APPLICATION

# Check the checksum of the decoded Developer ID Application certificate
echo "658613e0abe5c3187284e9662f18e1f0 $APPLE_CERT_DEVELOPER_APPLICATION" | md5sum -c -

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

# Import Developer ID Installer certificate to the keychain
security import $APPLE_CERT_DEVELOPER_APPLICATION -k $KEYCHAIN_PATH -P $APPLE_CERT_DEVELOPER_APPLICATION_PASSWORD -T /usr/bin/codesign -T /usr/bin/security -T /usr/bin/xcrun -T /usr/bin/productsign -T /usr/bin/productbuild

# Check if the import was successful
if [ $? -ne 0 ]; then
    echo "Error: Failed to import application certificate"
    exit 1
fi

# Inform the user about the successful import
echo "Certificates imported successfully to $KEYCHAIN_PATH"

# Clean up the temporary files
rm -f $APPLE_CERT_DEVELOPER_INSTALLER
rm -f $APPLE_CERT_DEVELOPER_APPLICATION

# Inform the user about the successful cleanup
echo "Temporary files cleaned up successfully"
