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

import com.google.api.client.util.Lists;
import com.google.api.services.datastore.client.DatastoreHelper;
import com.google.common.base.Throwables;

import com.spotify.asyncdatastoreclient.Batch;
import com.spotify.asyncdatastoreclient.Datastore;
import com.spotify.asyncdatastoreclient.DatastoreConfig;
import com.spotify.asyncdatastoreclient.DatastoreException;
import com.spotify.asyncdatastoreclient.Entity;
import com.spotify.asyncdatastoreclient.Insert;
import com.spotify.asyncdatastoreclient.KeyQuery;
import com.spotify.asyncdatastoreclient.Query;
import com.spotify.asyncdatastoreclient.QueryBuilder;
import com.spotify.asyncdatastoreclient.TransactionResult;

import java.util.Date;
import java.util.List;

import static com.spotify.asyncdatastoreclient.QueryBuilder.asc;
import static com.spotify.asyncdatastoreclient.QueryBuilder.eq;

/**
 * Some simple examples that should help you get started.
 */
public final class Example {

  private Example() {
  }

  private static void addData(final Datastore datastore) {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("inserted", new Date())
        .value("age", 40);
    try {
      datastore.execute(insert);
    } catch (final DatastoreException e) {
      System.err.println("Storage exception: " + Throwables.getRootCause(e).getMessage());
    }
  }

  private static void addDataInTransaction(final Datastore datastore) {
    try {
      final TransactionResult txn = datastore.transaction();

      final KeyQuery get = QueryBuilder.query("employee", 2345678L);
      final Entity existing = datastore.execute(get, txn).getEntity();

      // Check if the employee exists before inserting
      if (existing != null) {
        datastore.rollback(txn);
      } else {
        // Insert new employee inside a transaction
        final Insert insert = QueryBuilder.insert("employee", 2345678L)
            .value("fullname", "Fred Blinge")
            .value("inserted", new Date())
            .value("age", 40);
        datastore.execute(insert, txn);
      }
    } catch (final DatastoreException e) {
      System.err.println("Storage exception: " + Throwables.getRootCause(e).getMessage());
    }
  }

  private static void queryData(final Datastore datastore) {
    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(eq("age", 40))
        .orderBy(asc("fullname"));

    List<Entity> entities = Lists.newArrayList();
    try {
      entities = datastore.execute(get).getAll();
    } catch (final DatastoreException e) {
      System.err.println("Storage exception: " + Throwables.getRootCause(e).getMessage());
    }

    for (final Entity entity : entities) {
      System.out.println("Employee name: " + entity.getString("fullname"));
      System.out.println("Employee age: " + entity.getInteger("age"));
    }
  }

  private static void deleteData(final Datastore datastore) {
    final Batch delete = QueryBuilder.batch()
        .add(QueryBuilder.delete("employee", 1234567L))
        .add(QueryBuilder.delete("employee", 2345678L));
    try {
      datastore.execute(delete);
    } catch (final DatastoreException e) {
      System.err.println("Storage exception: " + Throwables.getRootCause(e).getMessage());
    }
  }

  public static void main(final String... args) throws Exception {
    final DatastoreConfig config = DatastoreConfig.builder()
        .connectTimeout(5000)
        .requestTimeout(1000)
        .maxConnections(5)
        .requestRetry(3)
        .dataset("my-dataset")
        .namespace("my-namespace")
        .credential(DatastoreHelper.getComputeEngineCredential())
        .build();

    final Datastore datastore = Datastore.create(config);

    // Add a single entity
    addData(datastore);

    // Add a single entity in a transaction
    addDataInTransaction(datastore);

    // Query the entities we've just inserted
    queryData(datastore);

    // Clean up entities
    deleteData(datastore);

    System.out.println("All complete.");
  }
}
