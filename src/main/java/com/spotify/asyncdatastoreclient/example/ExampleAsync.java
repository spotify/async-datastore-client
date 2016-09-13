/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.asyncdatastoreclient.example;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.asyncdatastoreclient.Batch;
import com.spotify.asyncdatastoreclient.Datastore;
import com.spotify.asyncdatastoreclient.DatastoreConfig;
import com.spotify.asyncdatastoreclient.Entity;
import com.spotify.asyncdatastoreclient.Insert;
import com.spotify.asyncdatastoreclient.KeyQuery;
import com.spotify.asyncdatastoreclient.MutationResult;
import com.spotify.asyncdatastoreclient.Query;
import com.spotify.asyncdatastoreclient.QueryBuilder;
import com.spotify.asyncdatastoreclient.QueryResult;
import com.spotify.asyncdatastoreclient.TransactionResult;

import java.util.Date;
import java.util.List;

import static com.spotify.asyncdatastoreclient.QueryBuilder.asc;
import static com.spotify.asyncdatastoreclient.QueryBuilder.eq;

/**
 * Some simple asynchronous examples that should help you get started.
 */
public final class ExampleAsync {

  private ExampleAsync() {
  }

  private static ListenableFuture<MutationResult> addData(final Datastore datastore) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("inserted", new Date())
        .value("age", 40);
    return datastore.executeAsync(insert);
  }

  private static ListenableFuture<MutationResult> addDataInTransaction(final Datastore datastore) {
    final ListenableFuture<TransactionResult> txn = datastore.transactionAsync();

    final KeyQuery get = QueryBuilder.query("employee", 2345678L);

    return Futures.transform(datastore.executeAsync(get, txn), (QueryResult result) -> {
      if (result.getEntity() == null) {
        datastore.rollbackAsync(txn); // fire and forget
        return Futures.immediateFuture(MutationResult.build());
      }

      final Insert insert = QueryBuilder.insert("employee", 2345678L)
          .value("fullname", "Fred Blinge")
          .value("inserted", new Date())
          .value("age", 40);
      return datastore.executeAsync(insert);
    });
  }

  private static ListenableFuture<QueryResult> queryData(final Datastore datastore) {
    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(eq("age", 40))
        .orderBy(asc("fullname"));
    return datastore.executeAsync(get);
  }

  private static ListenableFuture<MutationResult> deleteData(final Datastore datastore) {
    final Batch delete = QueryBuilder.batch()
        .add(QueryBuilder.delete("employee", 1234567L))
        .add(QueryBuilder.delete("employee", 2345678L));
    return datastore.executeAsync(delete);
  }

  public static void main(final String... args) throws Exception {
    final DatastoreConfig config = DatastoreConfig.builder()
        .connectTimeout(5000)
        .requestTimeout(1000)
        .maxConnections(5)
        .requestRetry(3)
        .project("my-dataset")
        .namespace("my-namespace")
        .credential(GoogleCredential.getApplicationDefault())
        .build();

    final Datastore datastore = Datastore.create(config);

    // Add a two entities asynchronously
    final ListenableFuture<MutationResult> addFirst = addData(datastore);
    final ListenableFuture<MutationResult> addSecond = addDataInTransaction(datastore);
    final ListenableFuture<List<Object>> addBoth = Futures.allAsList(addFirst, addSecond);

    // Query the entities we've just inserted
    final ListenableFuture<QueryResult> query = Futures.transform(addBoth, (List<Object> result) -> {
      return queryData(datastore);
    });

    // Print the query results before clean up
    final ListenableFuture<MutationResult> delete = Futures.transform(query, (QueryResult result) -> {
      for (final Entity entity : result) {
        System.out.println("Employee name: " + entity.getString("fullname"));
        System.out.println("Employee age: " + entity.getInteger("age"));
      }
      return deleteData(datastore);
    });

    Futures.addCallback(delete, new FutureCallback<MutationResult>() {
      @Override
      public void onSuccess(final MutationResult result) {
        System.out.println("All complete.");
      }

      @Override
      public void onFailure(final Throwable throwable) {
        System.err.println("Storage exception: " + Throwables.getRootCause(throwable).getMessage());
      }
    });
  }
}
