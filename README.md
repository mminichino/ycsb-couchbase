# Couchbase YCSB 3.0.0
This pacakge is a YCSB implementation to test against a Couchbase cluster. It uses YCSB core 0.18.0 and the Couchbase Java SDK v3.5.

## Requirements
- Java 17 or higher
- Linux, macOS, or Windows load generator system (8 cores / 32GB RAM minimum recommended)
- Couchbase Server or Couchbase Capella

## Quickstart

### 1. Start Couchbase Server
You will need a Couchbase cluster for testing. Please see either the documentation on [Couchbase Server](https://docs.couchbase.com/home/server.html) for self-managed deployments or [Couchbase Capella](https://docs.couchbase.com/cloud/index.html)
for public cloud based DBaaS deployments.

### 2. Set up YCSB
Download the distribution to begin testing.
```
curl -OLs https://github.com/couchbaselabs/ycsb-couchbase/releases/download/3.0.0/ycsb-couchbase.zip
```
```
unzip ycsb-couchbase.zip
```
```
cd ycsb-couchbase
```
On Linux/macOS:
```
vi conf/db.properties
```
On Windows:
```
notepad conf\db.properties
```
On Linux/macOS:
```
bin/ycsb-couchbase
```
On Windows:
```
bin\ycsb-couchbase.bat
```

### 3. Run a specific test

```
bin/ycsb-couchbase -w workloada
```

## Capella
The bucket automation for Capella uses the v4 public API. To use the automation, create an API key in the Capella UI and save it to a file named ```default-api-key-token.txt``` in a directory named ```.capella``` in either $HOME (Linux/macOS) or %userprofile% (Windows).

Uncomment the Capella properties in the config file and set them to match the project and database names as configured in Capella for the database you want to use for testing. When `capella.token` is set, the connection string is resolved through the Capella API and you do not need to set `couchbase.hostname`.

On Linux/macOS
```
% mkdir .capella
% vi .capella/default-api-key-token.txt
```

On Windows
```
C:\Users\Username> mkdir .capella
C:\Users\Username> cd .capella
C:\Users\Username\.capella> notepad default-api-key-token.txt
```

Edit properties
````
capella.project.name=ycsb-capella
capella.database.name=testdb01
capella.user.email=you@example.com
````

## Database Credentials
To use the automation, be sure to use credentials that have permission to create and mange buckets and indexes. While this test suite should not interfere with other buckets in a cluster, it will consume cluster resources, so it is advisable to make sure that the test operations are the only operations being performed against the cluster. It is also a best practice to not use a production cluster that is being used for production operations.

## Configuration Options
Under normal circumstances you should only need to set the hostname, username, and password properties. If you are using a cluster that was just built for testing with default settings then technically you just need to set the hostname parameter. For Capella, set `capella.token` (or use the token file described above), `capella.project.name`, `capella.database.name`, `capella.user.email`, and the database username and password properties.

Connection properties are provided by [couchbase-connect-sdk3](https://central.sonatype.com/artifact/com.codelry.util/couchbase-connect-sdk3) (currently 1.2.2).

| Property                          | Default                 | Description                                                                           |
|-----------------------------------|-------------------------|---------------------------------------------------------------------------------------|
| couchbase.hostname                | 127.0.0.1               | Connect hostname or IP address                                                        |
| couchbase.username                | Administrator           | Database credential username                                                          |
| couchbase.password                | password                | Database credential password or client cert KeyStore password                         |
| couchbase.ca.cert                 | None                    | Path to root CA PEM (cert validation is disabled if a CA isn't provided)              |
| couchbase.client.cert             | None                    | Path to client authentication KeyStore (PKCS12 or JKS format)                         |
| couchbase.keystore.type           | PKCS12                  | KeyStore type when using client certificate auth                                      |
| couchbase.sslMode                 | true                    | Use SSL (`true`) or do not use SSL (`false`)                                          |
| couchbase.quickConnect            | false                   | Skip cluster readiness checks for a faster connection                                 |
| couchbase.softFailure             | false                   | Log connection errors without throwing when `true`                                    |
| couchbase.debug                   | false                   | Enable debug logging for the connection layer                                         |
| couchbase.bucket                  | default                 | Test bucket name                                                                      |
| couchbase.scope                   | _default                | Test scope name                                                                       |
| couchbase.collection              | _default                | Test collection name                                                                  |
| couchbase.bucketType              | couchbase               | Bucket type for automation: `couchbase`, `ephemeral`, or `memcached`                  |
| couchbase.storageBackend          | couchstore              | Storage backend for automation: `couchstore` or `magma`                               |
| couchbase.replicaNum              | 1                       | Replica count used when creating the test bucket                                      |
| couchbase.ttlSeconds              | 0                       | Document TTL in seconds                                                               |
| couchbase.maxParallelism          | 0                       | Maximum parallelism for queries (`0` uses the SDK default)                            |
| couchbase.kvEndpoints             | 8                       | Number of KV endpoints for data operations                                            |
| couchbase.kvTimeout               | 5                       | Timeout in seconds for KV operations                                                  |
| couchbase.connectTimeout          | 15                      | Timeout in seconds for connections                                                    |
| couchbase.queryTimeout            | 75                      | Timeout in seconds for query operations                                               |
| couchbase.durability              | 0                       | Durability: None (0), Majority (1), Persist to Active (2), or Persist to Majority (3) |
| capella.token                     | None                    | Capella API v4 token; can also be loaded from the token file                          |
| capella.organization.name         | None                    | Capella organization name                                                             |
| capella.organization.id           | None                    | Capella organization ID                                                               |
| capella.project.name              | None                    | Capella project name                                                                  |
| capella.project.id                | None                    | Capella project ID                                                                    |
| capella.database.name             | None                    | Capella database name                                                                 |
| capella.database.id               | None                    | Capella database ID                                                                   |
| capella.user.email                | None                    | Capella user email for API authentication                                             |
| capella.user.id                   | None                    | Capella user ID for API authentication                                                |
| capella.api.host                  | None                    | Capella API host override                                                             |
