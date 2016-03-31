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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class QueryTest extends DatastoreTest {

  private final Random random = new Random();

  private String randomString(final int length) {
    return random.ints(random.nextInt(length + 1), 'a', 'z' + 1)
        .mapToObj((i) -> (char) i)
        .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
        .toString();
  }

  private void insertRandom(final int entities, final String kind) throws Exception {
    final List<String> role = ImmutableList.of("manager", "engineer", "sales", "marketing");
    for (int entity = 0; entity < entities; entity++) {
      final Insert insert = QueryBuilder.insert(kind, entity + 1)
          .value("fullname", randomString(20))
          .value("age", random.nextInt(60), false)
          .value("payroll", entity + 1)
          .value("senior", entity % 2 == 0)
          .value("role", role.get(entity % 4))
          .value("started", new Date());
      datastore.execute(insert);
    }
    waitForConsistency();
  }

  private static void waitForConsistency() throws Exception {
    // Ugly hack to minimise test failures due to inconsistencies.
    // An alternative, if running locally, is to this run `gcd` with `--consistency=1.0`
    Thread.sleep(300);
  }

  @Test
  public void testKeyQuery() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);
    waitForConsistency();

    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(1, entities.size());
    assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
    assertEquals(40, entities.get(0).getInteger("age").intValue());
  }

  @Test
  public void testMultiKeyQuery() throws Exception {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
      .value("fullname", "Fred Blinge")
      .value("age", 40, false);
    datastore.execute(insert1);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
      .value("fullname", "Jack Spratt")
      .value("age", 21);
    datastore.execute(insert2);
    waitForConsistency();

    final List<KeyQuery> keys = ImmutableList.of(
      QueryBuilder.query("employee", 1234567L), QueryBuilder.query("employee", 2345678L));
    final List<Entity> entities = datastore.execute(keys).getAll();
    assertEquals(2, entities.size());
    final List<Entity> sorted = entities
      .stream()
      .sorted((a, b) -> Long.compare(a.getKey().getId(), b.getKey().getId()))
      .collect(Collectors.toList());
    assertEquals("Fred Blinge", sorted.get(0).getString("fullname"));
    assertEquals(40, sorted.get(0).getInteger("age").intValue());
    assertEquals("Jack Spratt", sorted.get(1).getString("fullname"));
    assertEquals(21, sorted.get(1).getInteger("age").intValue());
  }

  @Test
  public void testKeyQueryNotExist() throws Exception {
    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final QueryResult getResult = datastore.execute(get);
    assertEquals(0, getResult.getAll().size());
    assertNull(getResult.getEntity());
  }

  @Test
  public void testKeyQueryBadKey() throws Exception {
    final KeyQuery get = QueryBuilder.query(Key.builder("incomplete").build());

    try {
      datastore.execute(get);
      fail("Expected DatastoreException exception.");
    } catch (final DatastoreException e) {
      assertEquals(400, e.getStatusCode().intValue()); // bad request
    }
  }

  @Test
  public void testSimpleQuery() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee");
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testQueryOrderAsc() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("payroll"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
    assertEquals(1, entities.get(0).getInteger("payroll").intValue());
    assertEquals(10, entities.get(9).getInteger("payroll").intValue());
  }

  @Test
  public void testQueryOrderDesc() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.desc("payroll"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
    assertEquals(10, entities.get(0).getInteger("payroll").intValue());
    assertEquals(1, entities.get(9).getInteger("payroll").intValue());
  }

  @Test
  public void testQueryOrderNotIndexed() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.desc("age"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(0, entities.size()); // non-indexed properties are ignored
  }

  @Test
  public void testQueryOrderNotExists() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("not_exists"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(0, entities.size()); // non-existing properties are ignored
  }

  @Test
  public void testQueryMultipleOrders() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("senior"))
        .orderBy(QueryBuilder.desc("payroll"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
    assertFalse(entities.get(0).getBoolean("senior"));
    assertTrue(entities.get(9).getBoolean("senior"));
    assertEquals(10, entities.get(0).getInteger("payroll").intValue());
    assertEquals(1, entities.get(9).getInteger("payroll").intValue());
  }

  @Test
  public void testQueryOrdersByKey() throws Exception {
    insertRandom(10, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee");
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
    assertEquals(1, entities.get(0).getKey().getId().intValue());
    assertEquals(10, entities.get(9).getKey().getId().intValue());
  }

  @Test
  public void testQueryEqFilter() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.eq("role", "engineer"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(5, entities.size());
    assertEquals("engineer", entities.get(0).getString("role"));
    assertEquals("engineer", entities.get(4).getString("role"));
  }

  @Test
  public void testQueryLtFilter() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.lt("payroll", 10));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(9, entities.size());
  }

  @Test
  public void testQueryLteFilter() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.lte("payroll", 10));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testQueryGtFilter() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.gt("payroll", 10));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testQueryGteFilter() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.gte("payroll", 10));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(11, entities.size());
  }

  @Test
  public void testQueryMultipleFilters() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.gte("payroll", 10))
        .filterBy(QueryBuilder.lte("payroll", 10));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(1, entities.size());
    assertEquals(10, entities.get(0).getInteger("payroll").intValue());
  }

  @Test
  public void testQueryFilterNotIndexed() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.lte("age", 40));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(0, entities.size());
  }

  @Test
  public void testQueryFilterNotExist() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.lte("not_exist", 40));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(0, entities.size());
  }

  @Test
  public void testQueryDateFilter() throws Exception {
    insertRandom(10, "employee");

    final Calendar today = Calendar.getInstance();
    today.set(Calendar.HOUR_OF_DAY, 0);
    today.set(Calendar.MINUTE, 0);
    today.set(Calendar.SECOND, 0);
    today.set(Calendar.MILLISECOND, 0);

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.gte("started", today.getTime()));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testQueryKeyFilter() throws Exception {
    final Key record = Key.builder("record", 2345678L).build();
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("record", record);
    datastore.execute(insert);
    waitForConsistency();

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.eq("record", record));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(1, entities.size());
  }

  @Test
  public void testQueryAncestorFilter() throws Exception {
    final Key employeeKey = Key.builder("employee", 1234567L).build();
    final Key salaryKey = Key.builder("payments", 222222L, employeeKey).build();

    final Insert insert = QueryBuilder.insert(salaryKey)
        .value("salary", 1000.00);
    datastore.execute(insert);
    waitForConsistency();

    final Query get = QueryBuilder.query()
        .kindOf("payments")
        .filterBy(QueryBuilder.ancestor(employeeKey));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(1, entities.size());
  }

  @Test
  public void testQueryGroupBy() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .groupBy(QueryBuilder.group("senior"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(2, entities.size());
  }

  @Test
  public void testQueryFilterAndGroupBy() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.eq("role", "engineer"))
        .groupBy(QueryBuilder.group("senior"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(1, entities.size());
  }

  @Test
  public void testQueryFilterAndGroupByAndOrderBy() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.gt("payroll", 10))
        .groupBy(QueryBuilder.group("payroll"))
        .orderBy(QueryBuilder.asc("payroll"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testProjectionQuery() throws Exception {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("payroll", 1000)
        .value("age", 40);
    datastore.execute(insert1);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
        .value("fullname", "Jack Spratt")
        .value("payroll", 1001)
        .value("age", 21);
    datastore.execute(insert2);
    waitForConsistency();

    final Query get = QueryBuilder.query()
        .properties("fullname", "payroll")
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("fullname"));
    final List<Entity> entities = datastore.execute(get).getAll();
    assertEquals(2, entities.size());

    assertEquals("Fred Blinge", entities.get(0).getString("fullname"));
    assertEquals(1000, entities.get(0).getInteger("payroll").intValue());
    assertNull(entities.get(0).getInteger("age"));
    assertEquals("Jack Spratt", entities.get(1).getString("fullname"));
    assertEquals(1001, entities.get(1).getInteger("payroll").intValue());
    assertNull(entities.get(1).getInteger("age"));
  }

  @Test
  public void testQueryKeysOnly() throws Exception {
    final Insert insert1 = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("payroll", 1000)
        .value("age", 40);
    datastore.execute(insert1);
    final Insert insert2 = QueryBuilder.insert("employee", 2345678L)
        .value("fullname", "Jack Spratt")
        .value("payroll", 1001)
        .value("age", 21);
    datastore.execute(insert2);
    waitForConsistency();

    final Query get = QueryBuilder.query()
        .keysOnly()
        .kindOf("employee")
        .orderBy(QueryBuilder.asc("fullname"));
    final QueryResult result = datastore.execute(get);
    final List<Entity> entities = result.getAll();
    assertEquals(2, entities.size());

    assertEquals(1234567L, entities.get(0).getKey().getId().longValue());
    assertNull(entities.get(0).getString("fullname"));
    assertNull(entities.get(0).getInteger("payroll"));
    assertNull(entities.get(0).getInteger("age"));
    assertEquals(2345678L, entities.get(1).getKey().getId().longValue());
    assertNull(entities.get(1).getString("fullname"));
    assertNull(entities.get(1).getInteger("payroll"));
    assertNull(entities.get(1).getInteger("age"));
  }

  @Test
  public void testQueryAsync() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.eq("role", "engineer"));

    final ListenableFuture<QueryResult> result = datastore.executeAsync(get);
    Futures.addCallback(result, new FutureCallback<QueryResult>() {
      @Override
      public void onSuccess(final QueryResult result) {
        assertEquals(5, result.getAll().size());
      }

      @Override
      public void onFailure(final Throwable throwable) {
        fail(Throwables.getRootCause(throwable).getMessage());
      }
    });
  }

  @Test
  public void testQueryIterator() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .filterBy(QueryBuilder.eq("role", "engineer"));

    int entityCount = 0;
    for (final Entity entity : datastore.execute(get)) {
      assertEquals("engineer", entity.getString("role"));
      entityCount++;
    }

    assertEquals(5, entityCount);
  }

  @Test
  public void testQueryLimit() throws Exception {
    insertRandom(20, "employee");

    final Query get = QueryBuilder.query()
        .kindOf("employee")
        .limit(10);

    final QueryResult result = datastore.execute(get);
    final List<Entity> entities = result.getAll();
    assertEquals(10, entities.size());
  }

  @Test
  public void testQueryPaged() throws Exception {
    insertRandom(100, "employee");

    Query get = QueryBuilder.query()
        .kindOf("employee")
        .limit(10);

    int total = 0;
    int batches = 0;

    while (true) {
      final QueryResult result = datastore.execute(get);
      final List<Entity> entities = result.getAll();
      if (entities.isEmpty()) {
        break;
      }

      total += entities.size();
      batches++;

      get = QueryBuilder.query()
          .fromCursor(result.getCursor())
          .kindOf("employee")
          .limit(10);
    }

    assertEquals(100, total);
    assertEquals(10, batches);
  }
}
