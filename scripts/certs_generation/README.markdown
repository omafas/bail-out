# Certificate generation

These scripts can be used for ca, intermediate and node certificates generation.
The generated certificates must not be used on production systems as it can be
unsecure. Please use it for test purposes only.

The scripts are interactive and are meant to be used by human. It will be hard
to use them from other scripts.

It's recommended to create a separate directory for cert generation.
Example:

```
$ mkdir certs
$ cd certs
```

## Generate cluster ca

```
./certs$ ~/cb/ns_server/scripts/certs_generation/generate_cluster_ca
Enter directory to put generated cluster ca to [./ca]:
Creating the root key...
Generating RSA private key, 4096 bit long modulus
........++
......................................++
e is 65537 (0x10001)
Enter pass phrase for ./certs/ca/private/ca.key.pem:
Verifying - Enter pass phrase ./certs/ca/private/ca.key.pem:
Creating the root certificate...
Enter pass phrase for ./certs/ca/private/ca.key.pem:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [US]:
State or Province Name [CA]:
Locality Name []:
Organization Name [Couchbase Inc.]:
Organizational Unit Name []:
Common Name []:Couchbase CA
Email Address []:
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number: 18392523557688755726 (0xff3f5eba5b0ac60e)
    Signature Algorithm: sha256WithRSAEncryption
        Issuer: C=US, ST=CA, O=Couchbase Inc., CN=Couchbase CA
        Validity
            Not Before: Oct 31 20:33:30 2019 GMT
            Not After : Oct 26 20:33:30 2039 GMT
        Subject: C=US, ST=CA, O=Couchbase Inc., CN=Couchbase CA
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                Public-Key: (4096 bit)
                Modulus:
                    00:da:06:a9:4b:f6:ba:1d:2b:2b:50:00:3f:63:a2:
                    fa:53:fc:e6:33:89:8d:11:c4:19:0d:d3:2d:d8:b3:
                    ea:2b:20:cc:24:5f:7f:89:9a:32:0d:df:47:9f:c0:
                    2b:98:1a:5e:b6:40:a5:93:3e:9c:79:00:3f:ba:07:
                    48:b3:47:c6:1a:1a:c8:27:16:d1:ad:7e:f5:34:18:
                    f9:ed:f8:d7:30:26:8e:7b:27:93:54:7e:77:78:a8:
                    84:16:03:f6:73:66:27:b2:09:0c:73:d5:8f:e6:a0:
                    56:1f:19:9e:b9:87:f4:67:08:d7:c0:90:d7:a1:2c:
                    bb:26:11:63:a4:3d:c8:1f:23:b5:ac:89:d7:a5:90:
                    e8:39:fb:d5:0d:26:ce:67:d3:53:e8:ec:02:6f:70:
                    cb:96:c3:68:2d:a2:e9:9f:c3:44:ac:c6:d5:7b:6b:
                    25:fe:80:23:8f:d7:fc:f3:ef:e8:f2:14:ea:41:e3:
                    e2:45:90:64:5c:14:61:ab:76:71:fe:a9:f8:3c:f4:
                    b8:e8:d2:9d:32:98:ee:96:8e:47:81:17:90:1c:a3:
                    bc:37:97:bd:1c:d8:21:f9:95:27:7a:d5:4b:da:fc:
                    05:f3:3c:4f:77:f7:38:03:82:4e:bd:b3:37:39:1b:
                    79:5e:70:06:6c:5b:21:00:1c:e4:f4:ee:62:28:8b:
                    92:2f:73:5c:99:6f:97:f5:e3:03:1c:e6:8c:a2:d0:
                    56:2a:ff:99:49:15:e3:b6:49:c3:b3:79:39:3e:7c:
                    88:74:5d:98:53:37:f5:1d:88:4e:03:48:40:03:64:
                    b3:8f:3b:b9:a8:be:5e:58:a2:7d:44:c0:0d:e7:5b:
                    99:4e:f6:7e:45:50:57:6c:e6:3f:de:69:2c:ef:c8:
                    e7:74:13:33:49:0c:aa:c9:02:83:d7:4c:a1:ac:27:
                    60:99:ab:ab:30:4d:fd:af:6e:74:d1:16:28:59:25:
                    47:44:3e:7b:7f:0c:e5:a7:76:f0:6f:29:c5:f8:24:
                    61:6c:96:c9:54:da:86:7d:af:10:2d:9d:0a:e3:e6:
                    69:a4:d7:91:6b:15:e7:ea:75:e1:89:2c:68:63:51:
                    1e:87:68:33:9b:09:c4:61:45:d5:fb:2b:bb:4d:3c:
                    23:b4:67:c7:7a:02:54:83:8c:c4:bd:91:22:b2:14:
                    00:2b:83:e2:c5:79:55:2b:1e:df:eb:82:3c:bc:74:
                    c5:67:06:e1:34:b1:4d:c0:12:35:87:e8:62:fc:60:
                    58:c3:83:7f:a9:ce:4f:35:9f:59:be:57:2f:42:18:
                    51:dc:04:9e:04:c2:e8:23:98:e9:10:fb:f8:bd:77:
                    fa:75:b7:53:dd:d3:d5:16:8d:45:c3:b9:57:c8:68:
                    cc:a3:95
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Subject Key Identifier:
                B1:E0:CC:7E:49:B8:FE:B2:1F:DE:40:06:89:9B:D4:39:DA:74:A1:B9
            X509v3 Authority Key Identifier:
                keyid:B1:E0:CC:7E:49:B8:FE:B2:1F:DE:40:06:89:9B:D4:39:DA:74:A1:B9

            X509v3 Basic Constraints: critical
                CA:TRUE
            X509v3 Key Usage: critical
                Digital Signature, Certificate Sign, CRL Sign
    Signature Algorithm: sha256WithRSAEncryption
         b2:8c:a1:3a:0a:20:be:e7:73:58:e8:36:9b:be:99:9d:9d:90:
         b3:ec:2f:d0:13:30:d0:fc:92:b0:05:9f:40:47:3b:f4:14:02:
         dc:e6:ec:b7:9a:f7:12:c8:b4:ba:d0:5a:4a:2e:eb:29:dc:dc:
         47:d6:5f:de:31:96:92:87:d6:ee:cb:f9:2e:06:39:7a:63:42:
         c5:18:71:ba:b2:9e:27:8e:7a:90:52:ee:bc:a4:85:01:22:fa:
         26:33:e2:82:f8:4d:e7:5d:46:38:cc:ce:e3:69:d9:85:43:f8:
         e9:e9:48:32:24:46:04:f5:93:fb:4e:5a:f7:d5:5b:bb:ee:ff:
         7d:91:38:4f:c5:1a:f1:5b:1b:5f:2f:fd:f7:4e:7a:b5:dd:4d:
         96:27:bd:15:11:a5:63:c9:e3:69:6f:e4:c0:69:13:b1:c0:fc:
         ae:f0:58:68:cc:bf:92:24:8b:b9:08:26:ad:f0:42:a8:66:90:
         7c:6e:c9:51:08:6d:4f:b7:71:a0:75:d6:35:9f:9b:06:6e:13:
         66:44:63:26:aa:3a:42:f4:55:a2:62:00:61:78:b5:f8:d4:38:
         c3:a3:cc:6d:ef:50:5b:e2:a0:c8:c7:7b:c8:b1:ee:ed:7a:e4:
         3e:29:b2:da:91:7f:06:3c:1a:19:dd:73:fe:58:e4:bc:bf:07:
         cd:4c:46:66:5f:c6:13:63:38:81:98:d9:c4:e1:9e:31:e7:f2:
         97:d1:f8:ba:45:50:8c:14:75:82:d4:9a:5c:fd:0a:2b:49:ea:
         96:61:d9:77:d2:bb:4f:ca:80:d2:9d:98:7d:4f:6f:1a:dc:97:
         78:3a:dc:1e:ef:78:0d:ff:97:e1:52:e8:02:20:c4:2d:b2:34:
         40:65:03:e5:af:09:d2:3f:6e:1f:a2:45:88:cb:01:27:00:b3:
         48:4a:8d:b8:81:6b:cc:24:0a:ea:79:af:31:9c:78:d4:df:fd:
         65:f1:19:22:c0:9f:30:67:4e:f7:4b:f0:6a:de:e7:d7:06:4a:
         84:02:50:c5:b3:98:fb:1b:5e:32:55:d1:aa:89:86:f2:d4:3c:
         90:bb:7b:c9:94:c6:b7:47:f0:d3:7d:d1:a5:d1:b0:39:e4:4c:
         5c:2e:53:31:68:96:e0:6b:e4:e0:99:64:44:93:aa:7a:f7:30:
         6a:2d:f9:ee:6f:f8:a6:c2:83:40:e4:0b:8d:37:a8:fa:2a:9b:
         66:6c:24:6a:54:81:6c:3a:9c:ad:b3:a0:e6:cd:e9:35:74:e8:
         c6:f8:f3:69:a9:8e:04:8f:db:5d:ee:00:c3:36:0b:ed:09:87:
         ed:0f:4b:e8:2d:05:92:0f:15:6b:6a:63:2f:ce:7e:41:2f:82:
         f1:03:92:06:ca:e6:e8:c7

Certificate: ./certs/ca/certs/ca.cert.pem
Private key: ./certs/ca/private/ca.key.pem

```

## Generate intermediate certificate

```
./certs$ ~/cn/ns_server/scripts/certs_generation/generate_intermediate_cert
Enter directory to put generated intermediate certificate to [./intermediate]:
Enter directory where root certificate (ca) is located [./ca]:
Creating the intermediate key...
Generating RSA private key, 4096 bit long modulus
.....................................................++
.........++
e is 65537 (0x10001)
Enter pass phrase for ./certs/intermediate/private/intermediate.key.pem:
Verifying - Enter pass phrase for ./certs/intermediate/private/intermediate.key.pem:
Create a certificate signing request...
Enter pass phrase for ./certs/intermediate/private/intermediate.key.pem:
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [US]:
State or Province Name [CA]:
Locality Name []:
Organization Name [Couchbase Inc.]:
Organizational Unit Name []:
Common Name []:Couchbase Intermediate Cert
Email Address []:
Creating the intermediate certificate...
Using configuration from ./certs/ca/root_conf.cnf
Enter pass phrase for ./certs/ca/private/ca.key.pem:
Check that the request matches the signature
Signature ok
Certificate Details:
        Serial Number: 4096 (0x1000)
        Validity
            Not Before: Oct 31 20:38:31 2019 GMT
            Not After : Oct 28 20:38:31 2029 GMT
        Subject:
            countryName               = US
            stateOrProvinceName       = CA
            organizationName          = Couchbase Inc.
            commonName                = Couchbase Intermediate Cert
        X509v3 extensions:
            X509v3 Subject Key Identifier:
                52:5C:C6:A7:B6:CC:9E:2C:24:EE:66:D6:3E:82:44:02:28:77:8A:3C
            X509v3 Authority Key Identifier:
                keyid:B1:E0:CC:7E:49:B8:FE:B2:1F:DE:40:06:89:9B:D4:39:DA:74:A1:B9

            X509v3 Basic Constraints: critical
                CA:TRUE, pathlen:0
            X509v3 Key Usage: critical
                Digital Signature, Certificate Sign, CRL Sign
Certificate is to be certified until Oct 28 20:38:31 2029 GMT (3650 days)
Sign the certificate? [y/n]:y


1 out of 1 certificate requests certified, commit? [y/n]y
Write out database with 1 new entries
Data Base Updated
Creating chain file...
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number: 4096 (0x1000)
    Signature Algorithm: sha256WithRSAEncryption
        Issuer: C=US, ST=CA, O=Couchbase Inc., CN=Couchbase CA
        Validity
            Not Before: Oct 31 20:38:31 2019 GMT
            Not After : Oct 28 20:38:31 2029 GMT
        Subject: C=US, ST=CA, O=Couchbase Inc., CN=Couchbase Intermediate Cert
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                Public-Key: (4096 bit)
                Modulus:
                    00:e3:26:d2:27:8c:85:f9:a3:5e:7d:ea:9b:2f:f4:
                    71:3e:34:0f:3e:44:98:06:c6:ea:58:23:8d:4d:79:
                    a7:a5:6e:15:87:d4:12:8e:1b:71:93:0a:bf:94:e0:
                    cf:fe:30:16:b9:e9:04:9f:87:d8:fd:b4:be:02:27:
                    df:e5:84:99:5a:8c:5d:d5:31:5c:d7:2c:51:68:ce:
                    44:03:6f:7e:11:c0:63:05:4a:4a:04:dd:53:30:92:
                    fe:48:ad:e1:05:e7:2c:fd:e1:b6:83:1f:36:c6:89:
                    13:26:ac:4a:1e:b3:eb:a0:8d:5e:99:92:c1:a5:36:
                    27:bc:65:c4:f7:2a:bb:78:4c:6e:90:5d:e6:3f:d0:
                    e7:64:71:a0:99:d5:7c:c6:ff:c6:d1:a1:ef:10:4c:
                    66:75:bd:cc:f3:10:2c:07:f1:ea:a6:d3:11:b4:d7:
                    73:7c:e2:93:8e:f4:5e:9e:07:7d:13:76:b7:01:b9:
                    e2:4e:23:4c:a5:ff:44:ec:bb:f6:17:f0:56:fb:5b:
                    60:42:20:91:48:9d:39:b6:6c:ed:9d:05:4e:d2:46:
                    b2:96:0e:a4:97:e6:0d:6b:83:03:67:2f:78:be:2b:
                    20:53:13:0b:48:dd:9e:05:d1:32:02:61:e0:51:ca:
                    42:f9:8f:f5:d4:a4:6c:a2:e9:58:be:df:b6:da:54:
                    bb:04:94:e6:f0:9d:02:2a:87:85:3f:74:6c:5e:14:
                    9f:9a:24:9e:ad:28:b4:05:2b:7a:6e:9f:59:b1:52:
                    0c:27:2a:67:58:03:e4:4f:fa:45:ae:e8:4d:a2:c8:
                    88:88:d6:43:1f:1d:d5:a2:3c:79:19:e5:95:34:2e:
                    81:5a:a3:d5:be:14:39:75:64:a2:68:8e:53:99:6f:
                    44:80:87:4f:d7:cd:bf:b0:e6:ac:99:08:8b:95:09:
                    37:50:af:5f:e9:1a:04:59:4d:da:ed:fc:5f:53:b6:
                    99:bf:a0:da:aa:f0:f4:a5:4b:29:31:ee:df:a7:95:
                    af:3d:ad:44:80:e2:76:ca:47:18:45:c8:e2:75:f4:
                    24:87:d4:db:0e:f2:34:75:71:bc:50:d1:e1:64:c3:
                    04:9a:ee:2f:11:ef:cd:05:7e:1a:86:d6:93:f9:1b:
                    27:72:ed:ac:e6:60:21:be:f8:8f:cf:70:c5:4f:37:
                    e7:a2:2e:6c:f5:1f:d5:42:1a:ec:d0:05:95:30:e4:
                    38:6e:e4:db:e7:5a:18:ac:e0:3d:b0:12:e6:08:65:
                    66:42:c3:4b:6f:27:f1:d9:26:74:33:0d:f0:81:02:
                    76:f9:46:19:fb:db:34:4d:58:8b:3d:fa:0d:6a:0b:
                    17:08:ef:1d:b9:7a:39:a2:fa:9d:86:d1:93:2d:c4:
                    3b:30:87
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Subject Key Identifier:
                52:5C:C6:A7:B6:CC:9E:2C:24:EE:66:D6:3E:82:44:02:28:77:8A:3C
            X509v3 Authority Key Identifier:
                keyid:B1:E0:CC:7E:49:B8:FE:B2:1F:DE:40:06:89:9B:D4:39:DA:74:A1:B9

            X509v3 Basic Constraints: critical
                CA:TRUE, pathlen:0
            X509v3 Key Usage: critical
                Digital Signature, Certificate Sign, CRL Sign
    Signature Algorithm: sha256WithRSAEncryption
         b4:2f:d6:e4:bc:17:28:7e:0a:2a:a8:41:4c:32:48:5e:d3:e9:
         bc:0d:f4:a2:7d:b8:47:10:07:09:ba:79:d3:ff:01:43:65:a9:
         99:28:a3:8a:eb:c4:c9:96:f9:7d:5e:43:8e:53:51:db:23:71:
         2c:c5:75:20:60:28:01:9c:09:64:4f:02:50:25:35:57:f3:b2:
         6b:d5:8f:a7:12:28:5a:60:04:73:21:3e:9b:e1:23:db:b0:5d:
         d0:1f:3f:0d:e9:54:2d:35:2d:28:d9:d8:9f:1e:ee:17:7a:95:
         2c:83:ff:ae:2b:fb:d6:cf:93:26:28:f4:e4:df:e4:ec:c6:15:
         f4:25:5f:14:a7:4b:8e:27:ff:c8:52:5b:18:6d:cc:3d:9c:49:
         29:71:67:5f:03:9e:c4:c5:c7:98:fb:a7:fa:0e:d6:99:b9:93:
         3f:da:60:ef:f3:39:10:a5:d3:fb:c5:57:1e:01:e5:69:a5:43:
         2b:3e:cc:25:76:18:96:81:a1:4f:3a:a0:5f:ed:bc:6f:28:21:
         d2:4c:47:6a:ff:84:1a:d9:0c:8c:aa:43:58:52:b0:08:63:c9:
         3e:ff:2b:26:4a:e6:cc:66:ac:c2:c7:08:19:73:66:c9:d4:2d:
         49:64:2e:62:b9:7e:b7:1f:3f:ee:9e:e8:82:47:47:a6:c0:ee:
         ff:1f:a6:4f:a0:c1:bc:dd:c5:a7:5c:00:6a:49:30:30:95:53:
         e9:88:ea:5d:69:3e:e6:88:ac:44:6d:8a:69:88:4c:a5:f3:ab:
         70:92:ab:7d:ed:f8:0b:37:fd:72:44:d8:a3:5d:60:e8:34:4f:
         fb:8d:18:e2:63:4e:21:89:27:7e:2c:89:d1:7c:ba:f8:1b:c4:
         3a:89:21:e9:07:ac:4a:2d:5b:bb:2a:8b:33:86:c7:85:c3:3c:
         13:03:d1:14:10:15:51:4a:eb:93:1b:35:0d:68:f5:eb:d9:a7:
         00:81:0a:e0:94:5d:75:57:f1:0e:0c:5f:09:eb:58:34:c7:11:
         9c:04:81:64:f1:76:71:b0:89:26:1a:ab:f7:31:5d:1e:84:56:
         6c:5e:aa:d9:b0:ee:06:f6:9e:a8:23:be:f6:74:c8:2b:0c:a7:
         08:b4:d0:c7:f1:f8:ee:44:52:6e:10:64:80:5a:03:af:70:89:
         14:a6:8e:be:28:8a:c8:eb:7f:54:d9:27:87:bf:11:3b:cb:cd:
         6f:93:43:25:f9:58:b5:18:1a:ad:a3:6c:74:97:2e:3b:50:00:
         12:07:0f:43:3e:6b:f4:ec:6c:5f:1f:91:8e:1d:78:68:68:99:
         20:f4:8e:96:17:25:5e:5c:6c:67:9a:7f:dd:0a:34:e7:b0:a7:
         e1:05:cc:9b:dd:f8:f0:19
Verifying the intermediate certificate against the root certificate...
./certs/intermediate/certs/intermediate.cert.pem: OK

Intermediate certificate: ./certs/intermediate/certs/intermediate.cert.pem
Intermediate private key: ./certs/intermediate/private/intermediate.key.pem
Chain (CA + Intermediate certs): ./certs/intermediate/certs/ca-chain.cert.pem
```

## Generate node certificates

```
./certs$ ~/cb/ns_server/scripts/certs_generation/generate_node_cert
Enter directory to put generated node certificates to [./nodes]:
Enter node name (hostname): node0.localhost
Enter directory where intermediate certificate is located [./intermediate]:
Creating a key...
Generating RSA private key, 2048 bit long modulus
...+++
.....................+++
e is 65537 (0x10001)
Creating a certificate signing request...
You are about to be asked to enter information that will be incorporated
into your certificate request.
What you are about to enter is what is called a Distinguished Name or a DN.
There are quite a few fields but you can leave some blank
For some fields there will be a default value,
If you enter '.', the field will be left blank.
-----
Country Name (2 letter code) [US]:
State or Province Name [CA]:
Locality Name []:
Organization Name [Couchbase Inc.]:
Organizational Unit Name []:
Common Name []:node0.localhost
Email Address []:
Creating a certificate...
Using configuration from ./certs/intermediate/intermediate_conf.cnf
Enter pass phrase for ./certs/intermediate/private/intermediate.key.pem:
Check that the request matches the signature
Signature ok
Certificate Details:
        Serial Number: 4096 (0x1000)
        Validity
            Not Before: Oct 31 20:42:51 2019 GMT
            Not After : Jan 26 20:42:51 2029 GMT
        Subject:
            countryName               = US
            stateOrProvinceName       = CA
            organizationName          = Couchbase Inc.
            commonName                = node0.localhost
        X509v3 extensions:
            X509v3 Basic Constraints:
                CA:FALSE
            Netscape Cert Type:
                SSL Server
            Netscape Comment:
                OpenSSL Generated Server Certificate
            X509v3 Subject Key Identifier:
                11:31:32:F4:60:7A:BE:D4:8E:7B:9B:43:09:04:40:50:9E:4A:CF:A3
            X509v3 Authority Key Identifier:
                keyid:52:5C:C6:A7:B6:CC:9E:2C:24:EE:66:D6:3E:82:44:02:28:77:8A:3C
                DirName:/C=US/ST=CA/O=Couchbase Inc./CN=Couchbase CA
                serial:10:00

            X509v3 Key Usage: critical
                Digital Signature, Key Encipherment
            X509v3 Extended Key Usage:
                TLS Web Server Authentication
Certificate is to be certified until Jan 26 20:42:51 2029 GMT (3375 days)
Sign the certificate? [y/n]:y


1 out of 1 certificate requests certified, commit? [y/n]y
Write out database with 1 new entries
Data Base Updated
Please verify the certificate:
Certificate:
    Data:
        Version: 3 (0x2)
        Serial Number: 4096 (0x1000)
    Signature Algorithm: sha256WithRSAEncryption
        Issuer: C=US, ST=CA, O=Couchbase Inc., CN=Couchbase Intermediate Cert
        Validity
            Not Before: Oct 31 20:42:51 2019 GMT
            Not After : Jan 26 20:42:51 2029 GMT
        Subject: C=US, ST=CA, O=Couchbase Inc., CN=node0.localhost
        Subject Public Key Info:
            Public Key Algorithm: rsaEncryption
                Public-Key: (2048 bit)
                Modulus:
                    00:97:d6:1b:d1:8e:ab:0c:32:49:1c:a9:e4:14:13:
                    b2:31:3b:7c:2b:f7:b7:bb:09:ec:c3:10:db:d0:da:
                    d0:1b:f7:8d:a9:d9:77:e3:09:05:a9:63:ff:c0:c8:
                    1f:15:dd:fc:39:ad:9e:9f:89:5a:8f:63:0a:7b:9e:
                    b0:97:1a:f6:d2:63:61:13:4d:60:55:c7:46:b3:02:
                    c7:81:2b:32:1d:62:4f:e0:3d:01:0c:45:39:ec:77:
                    3a:50:e0:fa:70:e2:87:df:75:5b:25:df:60:10:ac:
                    ac:e5:b5:5f:b8:0b:0d:66:04:e4:b4:bb:7e:90:2e:
                    8a:09:1f:3a:3c:2f:89:4c:a2:54:76:7f:d9:2e:c9:
                    99:2b:7c:0e:be:29:46:3d:1a:ae:5b:42:9d:ca:aa:
                    1e:e0:49:b4:a1:e2:d1:62:cc:93:93:69:90:d0:24:
                    bc:a2:67:bc:68:64:0a:cc:03:6d:e3:23:b0:2c:16:
                    87:df:0b:53:5e:c1:f4:ab:ac:41:b9:04:d5:ec:36:
                    c7:11:bd:ef:80:77:fd:6a:01:22:a4:22:b0:19:7f:
                    0f:ae:e5:d5:9c:e8:9e:0d:4b:59:5c:cf:a4:0f:c5:
                    98:a5:90:9a:02:48:79:b6:6f:20:43:7f:bf:72:6c:
                    7e:17:fd:e7:73:73:bb:5a:8f:2f:52:ce:5d:fb:3e:
                    bd:db
                Exponent: 65537 (0x10001)
        X509v3 extensions:
            X509v3 Basic Constraints:
                CA:FALSE
            Netscape Cert Type:
                SSL Server
            Netscape Comment:
                OpenSSL Generated Server Certificate
            X509v3 Subject Key Identifier:
                11:31:32:F4:60:7A:BE:D4:8E:7B:9B:43:09:04:40:50:9E:4A:CF:A3
            X509v3 Authority Key Identifier:
                keyid:52:5C:C6:A7:B6:CC:9E:2C:24:EE:66:D6:3E:82:44:02:28:77:8A:3C
                DirName:/C=US/ST=CA/O=Couchbase Inc./CN=Couchbase CA
                serial:10:00

            X509v3 Key Usage: critical
                Digital Signature, Key Encipherment
            X509v3 Extended Key Usage:
                TLS Web Server Authentication
    Signature Algorithm: sha256WithRSAEncryption
         b4:89:23:78:0a:8d:4b:b7:0a:01:60:b7:7b:1a:90:c4:d5:27:
         59:da:76:06:0d:ed:63:51:61:79:16:1f:be:bf:09:ef:d6:24:
         c1:7d:4b:f1:55:ba:be:24:41:e1:78:88:cf:bd:a2:0d:b8:b0:
         91:b1:b1:6b:0f:67:08:a0:54:8b:9d:66:2e:1c:87:d8:5b:b2:
         4c:83:c7:63:f5:27:f1:9d:a4:88:9b:26:72:62:29:14:6d:3e:
         19:34:f4:64:a3:79:03:d4:9e:18:c1:d0:23:cd:60:38:61:57:
         88:87:f7:b6:f9:28:af:e3:d5:05:63:29:eb:32:e1:e0:ff:cf:
         30:66:b3:66:6f:55:35:49:3a:e8:cd:38:34:6d:a6:2c:0b:3d:
         00:d7:88:db:3c:29:3c:30:0c:a1:c6:c4:b9:5f:f9:35:21:67:
         5b:3a:2e:88:0d:6f:ee:1e:73:d7:3e:d6:a0:51:4f:82:09:ba:
         8d:b3:9f:8f:a6:98:eb:31:30:e7:ac:03:0b:9b:32:e5:92:61:
         31:e0:0c:38:cb:b8:62:9f:08:ae:94:a9:e4:14:c8:03:0b:57:
         14:ed:4b:04:91:ee:ef:0e:6d:a1:e2:36:0a:6e:cb:00:f2:70:
         fa:7a:9d:35:65:8a:a7:b6:71:7a:0c:9b:74:d1:10:1c:0e:2a:
         ad:c4:ad:d0:4c:78:9b:28:75:8b:9c:d1:eb:0e:5b:85:7d:77:
         49:84:cc:47:7f:ec:d6:ce:a3:eb:c5:e3:ed:63:8a:a5:d7:5d:
         3a:f3:6b:0f:e8:9b:5a:a4:48:84:60:79:90:70:5b:0c:bd:73:
         c2:71:48:6b:5c:73:87:a1:7e:e9:90:f2:87:97:78:5e:18:4f:
         06:c7:06:7f:1a:23:f2:a1:3d:28:67:60:d1:ae:45:43:a4:31:
         b4:55:cd:c4:62:4d:7e:47:6f:77:4e:5c:3e:c6:fa:56:0d:86:
         4c:76:7c:d1:ed:74:e5:ff:c4:4f:5d:a4:25:10:4e:94:c2:e6:
         6f:12:bc:e3:58:93:b1:c6:7d:33:e1:3d:64:53:c8:3e:cd:6b:
         ae:8c:94:ac:cc:2e:52:8a:c0:17:b1:8e:51:09:00:40:26:58:
         b4:98:f6:71:34:f6:0d:23:f1:1d:bd:f5:c4:50:63:13:91:c6:
         ac:eb:ba:98:df:62:46:63:7d:c7:15:58:7e:b7:85:03:48:f0:
         d5:46:f7:ac:d5:29:48:cb:03:3c:7a:19:d2:47:db:02:bb:01:
         9a:72:7c:ea:d0:4a:64:e8:95:79:20:e3:7d:bb:1a:27:e9:8a:
         b2:9e:59:60:ec:8b:5c:9a:ca:13:76:61:0d:7f:9f:db:31:cb:
         09:a8:66:77:01:c2:8b:9a
Validating chain of trust...
./certs/nodes/node0.localhost/node0.localhost.cert.pem: OK

Node key:  ./certs/nodes/node0.localhost/private/node0.localhost.key.pem
Node cert: ./certs/nodes/node0.localhost/node0.localhost.cert.pem
Files to be copied to couchbase node inbox: ./certs/nodes/node0.localhost/inbox/*
```
