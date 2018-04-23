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
  <version>3.0.2</version>
</dependency>
```

> NOTE: Version 3.0.0+ depends on Guava 19 which contains breaking changes to `Futures.transform`.
If you require support for Guava version 18 or lower then use async-datastore-client version 2.1.0.

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
    .project(PROJECT_ID)
    .credential(GoogleCredential
        .fromStream(credentialsInputStream)
        .createScoped(DatastoreConfig.SCOPES))
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
on port 8080. To run tests, first download the [Development Server](https://cloud.google.com/datastore/docs/downloads), at least version
1.4.1 and start the emulator:

```sh
gcloud beta emulators datastore start --host-port localhost:8080 --consistency 1.0 --project async-test --data-dir project-test
```

> NOTE: The `--consistency=1.0` option is sometimes necessary in order
for unit tests to run successful.

All integration tests may by run with maven as follows:

```sh
mvn verify
```

Properties may also be provided to override unit test configuration:

```sh
mvn verify -Dhost=https://www.googleapis.com -Dproject=testing -Dkeypath=./my-key.json
```

## License

This software is released under the Apache License 2.0. More information in
the file LICENSE distributed with this project.
