# Asynchronous Google Datastore Client

A modern, feature-rich and tunable Java client library for [Google Cloud Datastore](https://cloud.google.com/datastore/docs/concepts/overview).

## Features

- Supports a asynchronous (non blocking) design that returns
[ListenableFutures](https://code.google.com/p/guava-libraries/wiki/ListenableFutureExplained) for all queries and mutations.
- Also supports synchronous alternatives.
- Insulates the consumer from having to deal with Protobuf payloads.
- Includes a simple `QueryBuilder` to construct natural looking queries.

## Overview

The current implementations of [Google Datastore Client](https://github.com/GoogleCloudPlatform/google-cloud-datastore)
and [Google Cloud Java Client](https://github.com/GoogleCloudPlatform/gcloud-java)
are synchronous, meaning they block when making HTTP calls to their backend.
This client uses [async-http-client](https://github.com/AsyncHttpClient/async-http-client)
and returns `ListenableFutures` which can be nicer to work with, especially
running at scale.

## Usage

Add this to your pom.xml file
```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>async-datastore-client</artifactId>
  <version>1.0.1</version>
</dependency>
```

### Example: Insert an entity

```java
import com.spotify.asyncdatastoreclient.DatastoreConfig;
import com.spotify.asyncdatastoreclient.Datastore;
import com.spotify.asyncdatastoreclient.QueryBuilder;
import com.spotify.asyncdatastoreclient.Insert;
import com.spotify.asyncdatastoreclient.MutationResult;

import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

final DatastoreConfig config = DatastoreConfig.builder()
    .requestTimeout(1000)
    .requestRetry(3)
    .dataset(DATASET_ID);
    .credential(DatastoreHelper.getServiceAccountCredential(ACCOUNT, KEY_PATH))
    .build();

final Datastore datastore = Datastore.create(config);

final Insert insert = QueryBuilder.insert("employee", 1234567L)
    .value("fullname", "Fred Blinge")
    .value("age", 40)
    .value("workdays", ImmutableList.of("Monday", "Tuesday", "Friday"));

// for asynchronous call...
final ListenableFuture<MutationResult> resultAsync = datastore.executeAsync(insert);

// ...or for synchronous
final MutationResult result = datastore.execute(insert);
```

### Example: Query entities

```java
import com.spotify.asyncdatastoreclient.QueryBuilder;
import com.spotify.asyncdatastoreclient.Query;

import static com.spotify.asyncdatastoreclient.QueryBuilder.eq;
import static com.spotify.asyncdatastoreclient.QueryBuilder.asc;

final Query query = QueryBuilder.query()
    .kindOf("employee")
    .filterBy(eq("role", "engineer"))
    .orderBy(asc("age"));

// call datastore.executeAsync() to get a ListenableFuture<QueryResult>
for (final Entity entity : datastore.execute(query)) {
  System.out.println("Name: " + entity.getString("fullname));
  ...
}
```

## Building

```sh
mvn clean compile
```

## Running tests

By default integration tests are executed against a [Local Development Server](https://cloud.google.com/datastore/docs/tools/devserver)
on port 8080. To run tests, first download the [Development Server](https://cloud.google.com/datastore/docs/downloads)
and create the local datastore as follows:

```sh
gcd.sh create -d async-test test-project
```

This will create a project called `test-project` with a dataset ID of
`async-test`. You then start the server as follows:

```sh
gcd.sh start --consistency=1.0 test-project
```

> NOTE: The `--consistency=1.0` option is sometimes necessary in order
for unit tests to run successful.

All integration tests may by run with maven as follows:

```sh
mvn verify
```

Properties may also be provided to override unit test configuration:

```sh
mvn verify -Dhost=https://www.googleapis.com -Ddataset=testing -Daccount=abc@developer.gserviceaccount.com -Dkeypath=./my-key-8ae3ab23d37.p12
```

## License

This software is released under the Apache License 2.0. More information in
the file LICENSE distributed with this project.
