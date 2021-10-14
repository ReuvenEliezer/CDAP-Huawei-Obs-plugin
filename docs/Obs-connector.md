# Huawei Obs Connection


Description
-----------
Use this connection to access data in Huawei Obs.

Properties
----------
**Name:** Name of the connection. Connection names must be unique in a namespace.

**Description:** Description of the connection.

**Authentication Method:** Authentication method to access Obs. The default value is Access Credentials.
IAM can only be used if the plugin is run in an AWS environment, such as on EMR.

**Access Key:** Huawei access key required for authentication.

**Secret Key:** Huawei secret key required for authentication.

**End Point:** End-Point to be used by the Obs Client.

Path of the connection
----------------------
To browse, get a sample from, or get the specification for this connection through
[Pipeline Microservices](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/975929350/Pipeline+Microservices), the `path`
property is required in the request body. It's an absolute Amazon S3 path of a file or folder.