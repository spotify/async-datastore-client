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

package com.spotify.asyncdatastoreclient;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class TransactionTest extends DatastoreTest {

  @Test
  public void testInsert() throws Exception {
    final TransactionResult txn = datastore.transaction();

    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final MutationResult insertResult = datastore.execute(insert, txn);

    final KeyQuery get = QueryBuilder.query(insertResult.getInsertKey());
    final QueryResult getResult = datastore.execute(get);

    assertEquals("Fred Blinge", getResult.getEntity().getString("fullname"));
    assertEquals(40, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testInsertAsync() throws Exception {
    final ListenableFuture<TransactionResult> txn = datastore.transactionAsync();

    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final ListenableFuture<MutationResult> insertResult = datastore.executeAsync(insert, txn);

    final ListenableFuture<QueryResult> getResult = Futures.transformAsync(insertResult, mutationResult -> {
      final KeyQuery get = QueryBuilder.query(mutationResult.getInsertKey());
      return datastore.executeAsync(get);
    });

    Futures.addCallback(getResult, new FutureCallback<QueryResult>() {
      @Override
      public void onSuccess(final QueryResult result) {
        assertEquals("Fred Blinge", result.getEntity().getString("fullname"));
        assertEquals(40, result.getEntity().getInteger("age").intValue());
      }

      @Override
      public void onFailure(final Throwable throwable) {
        fail(Throwables.getRootCause(throwable).getMessage());
      }
    });
  }

  @Test
  public void testGetThenInsert() throws Exception {
    final TransactionResult txn = datastore.transaction();

    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final QueryResult getResult = datastore.execute(get, txn);
    assertEquals(0, getResult.getAll().size());

    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final MutationResult insertResult = datastore.execute(insert, txn);
    assertTrue(insertResult.getIndexUpdates() > 0);
  }

  @Test
  public void testTransactionExpired() throws Exception {
    final TransactionResult txn = datastore.transaction();

    final Insert insertFirst = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    datastore.execute(insertFirst, txn);

    final Insert insertSecond = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    try {
      datastore.execute(insertSecond, txn);
      fail("Expected DatastoreException exception.");
    } catch (final DatastoreException e) {
      assertEquals(400, e.getStatusCode().intValue()); // bad request
    }
  }

  @Test
  public void testTransactionWriteConflict() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);

    final TransactionResult txn = datastore.transaction();
    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final QueryResult getResult = datastore.execute(get, txn);
    assertNotNull(getResult.getEntity());

    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41, false);
    datastore.execute(update); // update outside transaction

    try {
      datastore.execute(update, txn); // update inside transaction
      fail("Expected DatastoreException exception.");
    } catch (final DatastoreException e) {
      assertEquals(409, e.getStatusCode().intValue()); // conflict
    }
  }

  @Test
  public void testTransactionRead() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);

    final TransactionResult txn = datastore.transaction();
    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final QueryResult getResult1 = datastore.execute(get, txn);
    assertEquals(40, getResult1.getEntity().getInteger("age").intValue());

    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41, false);
    datastore.execute(update); // update outside transaction

    final QueryResult getResult2 = datastore.execute(get, txn); // read inside transaction
    assertEquals(40, getResult2.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testInsertBatchInTransaction() throws Exception {
    final TransactionResult txn = datastore.transaction();
    final Key parent = Key.builder("parent", "root").build();

    final Insert insert1 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Jack Spratt")
        .value("age", 21, false);

    final Insert insert2 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final Insert insert3 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Harry Ramsdens")
        .value("age", 50, false);

    final Batch batch = QueryBuilder.batch()
        .add(insert1)
        .add(insert2)
        .add(insert3);

    final MutationResult result = datastore.execute(batch, txn);
    assertFalse(result.getInsertKeys().isEmpty());

    final Query getAll = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.ancestor(parent))
        .orderBy(QueryBuilder.asc("fullname"));
    final List<Entity> entities = datastore.execute(getAll).getAll();

    assertEquals(3, entities.size());
    assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
    assertEquals("Harry Ramsdens", entities.get(1).getString("fullname"));
    assertEquals("Jack Spratt", entities.get(2).getString("fullname"));
  }

  @Test
  public void testQueryInTransaction() throws Exception {
    final Key parent = Key.builder("parent", "root").build();

    final Insert insert1 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Jack Spratt")
        .value("age", 21, false);

    final Insert insert2 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final Insert insert3 = QueryBuilder.insert(Key.builder("employee", parent).build())
        .value("fullname", "Harry Ramsdens")
        .value("age", 50, false);

    final Batch batch = QueryBuilder.batch()
        .add(insert1)
        .add(insert2)
        .add(insert3);

    final MutationResult result = datastore.execute(batch);
    assertFalse(result.getInsertKeys().isEmpty());

    final TransactionResult txn = datastore.transaction();
    final Query getAll = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.ancestor(parent))
        .orderBy(QueryBuilder.asc("fullname"));
    final List<Entity> entities = datastore.execute(getAll, txn).getAll();

    assertEquals(3, entities.size());
    assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
    assertEquals("Harry Ramsdens", entities.get(1).getString("fullname"));
    assertEquals("Jack Spratt", entities.get(2).getString("fullname"));

    datastore.commit(txn);
  }
}
