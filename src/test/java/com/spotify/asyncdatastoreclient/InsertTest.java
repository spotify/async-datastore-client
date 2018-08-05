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
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Date;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class InsertTest extends DatastoreTest {

  @Test
  public void testInsert() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final MutationResult result = datastore.execute(insert);
    assertTrue(result.getIndexUpdates() > 0);
  }

  @Test
  public void testInsertAuto() throws Exception {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final MutationResult result = datastore.execute(insert);
    assertTrue(result.getIndexUpdates() > 0);
    assertEquals("employee", result.getInsertKey().getKind());
    assertTrue(result.getInsertKey().getId() > 0);
  }

  @Test
  public void testInsertAsync() throws Exception {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final ListenableFuture<MutationResult> result = datastore.executeAsync(insert);
    Futures.addCallback(result, new FutureCallback<MutationResult>() {
      @Override
      public void onSuccess(final MutationResult result) {
        assertEquals("employee", result.getInsertKey().getKind());
        assertTrue(result.getInsertKey().getId() > 0);
      }

      @Override
      public void onFailure(final Throwable throwable) {
        fail(Throwables.getRootCause(throwable).getMessage());
      }
    }, MoreExecutors.directExecutor());
  }

  @Test
  public void testInsertEntity() throws Exception {
    final Date now = new Date();
    final ByteString picture = ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});
    final Entity address = Entity.builder("address", 222222L)
        .property("first_line", "22 Arcadia Ave")
        .property("zipcode", "90210").build();

    final Entity entity = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("nickname", "Freddie", false)
        .property("height", 2.43)
        .property("holiday_allowance", 22.5, false)
        .property("payroll_number", 123456789)
        .property("age", 40, false)
        .property("senior_role", false)
        .property("active", true, false)
        .property("start_date", now)
        .property("update_date", now, false)
        .property("picture", picture)
        .property("address", address)
        .property("manager", Key.builder("employee", 234567L).build())
        .property("workdays", ImmutableList.of("Monday", "Tuesday", "Friday"))
        .property("overtime_hours", ImmutableList.of(2, 3, 4))
        .build();

    final Insert insert = QueryBuilder.insert(entity);
    final MutationResult result = datastore.execute(insert);
    assertFalse(result.getInsertKeys().isEmpty());
  }

  @Test
  public void testInsertAlreadyExists() throws Exception {
    final Insert insertFirst = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insertFirst);

    final Insert insertSecond = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Jack Spratt")
        .value("age", 21, false);

    try {
      datastore.execute(insertSecond);
      fail("Expected DatastoreException exception.");
    } catch (final DatastoreException e) {
      assertEquals(409, e.getStatusCode().intValue()); // conflict
    }
  }

  @Test
  public void testInsertBlob() throws Exception {
    final byte[] randomBytes = new byte[2000];
    new Random().nextBytes(randomBytes);
    final ByteString large = ByteString.copyFrom(randomBytes);
    final ByteString small = ByteString.copyFrom(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9});

    final Insert insert = QueryBuilder.insert("employee")
        .value("picture1", small)
        .value("picture2", large, false);

    final MutationResult result = datastore.execute(insert);
    assertFalse(result.getInsertKeys().isEmpty());
  }

  @Test
  public void testInsertAutoThenGet() throws Exception {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    final MutationResult insertResult = datastore.execute(insert);

    final KeyQuery get = QueryBuilder.query(insertResult.getInsertKey());
    final QueryResult getResult = datastore.execute(get);

    assertEquals("Fred Blinge", getResult.getEntity().getString("fullname"));
    assertEquals(40, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testInsertFullThenGet() throws Exception {
    final Key key = Key.builder("employee", 1234567L).build();
    final Insert insert = QueryBuilder.insert(key)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);

    final KeyQuery get = QueryBuilder.query(key);
    final QueryResult getResult = datastore.execute(get);

    assertEquals("employee", getResult.getEntity().getKey().getKind());
    assertEquals(1234567L, getResult.getEntity().getKey().getId().longValue());
  }

  @Test
  public void testInsertWithParent() throws Exception {
    final Key employeeKey = Key.builder("employee", 1234567L).build();
    final Key salaryKey = Key.builder("payments", 222222L, employeeKey).build();

    final Insert insert = QueryBuilder.insert(salaryKey)
        .value("salary", 1000.00);
    datastore.execute(insert);

    final KeyQuery get = QueryBuilder.query(salaryKey);
    final QueryResult getResult = datastore.execute(get);

    assertEquals("employee", getResult.getEntity().getKey().getPath().get(0).getKind());
    assertEquals(1234567L, getResult.getEntity().getKey().getPath().get(0).getId().longValue());
    assertEquals("payments", getResult.getEntity().getKey().getPath().get(1).getKind());
    assertEquals(222222L, getResult.getEntity().getKey().getPath().get(1).getId().longValue());
  }

  @Test
  public void testInsertBatch() throws Exception {
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
}
