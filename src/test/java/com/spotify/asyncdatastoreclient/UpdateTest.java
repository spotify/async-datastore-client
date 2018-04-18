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

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

@Category(IntegrationTest.class)
public class UpdateTest extends DatastoreTest {

  @Test
  public void testUpdateExisting() throws Exception {
    final Insert insert = QueryBuilder.insert("employee")
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);

    final MutationResult existingResult = datastore.execute(insert);

    final Update update = QueryBuilder.update(existingResult.getInsertKey())
        .value("age", 41);
    final MutationResult updateResult = datastore.execute(update);
    assertTrue(updateResult.getIndexUpdates() > 0);

    final KeyQuery get = QueryBuilder.query(existingResult.getInsertKey());
    final QueryResult getResult = datastore.execute(get);

    assertEquals(41, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testUpdateNotExisting() throws Exception {
    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41);

    try {
      datastore.execute(update);
      fail("Expected DatastoreException exception.");
    } catch (final DatastoreException e) {
      assertEquals(404, e.getStatusCode().intValue()); // not found
    }
  }

  @Test
  public void testUpdateNotExistingWithUpsert() throws Exception {
    final Update update = QueryBuilder.update("employee", 1234567L)
        .value("age", 41)
        .upsert();

    final MutationResult updateResult = datastore.execute(update);
    assertTrue(updateResult.getIndexUpdates() > 0);

    final KeyQuery get = QueryBuilder.query("employee", 1234567L);
    final QueryResult getResult = datastore.execute(get);

    assertEquals(41, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testUpdateEntity() throws Exception {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("age", 40, false)
        .build();
    final MutationResult result = datastore.execute(QueryBuilder.insert(existing));
    final Key existingKey = result.getInsertKey();

    final Entity entity = Entity.builder(existing)
        .key(existingKey)
        .property("age", 41)
        .build();

    final Update update = QueryBuilder.update(entity);
    final MutationResult updateResult = datastore.execute(update);
    assertTrue(updateResult.getIndexUpdates() > 0);

    final KeyQuery get = QueryBuilder.query(entity.getKey());
    final QueryResult getResult = datastore.execute(get);

    assertEquals(41, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testUpdateAddProperty() throws Exception {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .build();
    final MutationResult result = datastore.execute(QueryBuilder.insert(existing));
    final Key existingKey = result.getInsertKey();

    final Entity entity = Entity.builder(existing)
        .key(existingKey)
        .property("age", 40)
        .build();

    final Update update = QueryBuilder.update(entity);
    final MutationResult updateResult = datastore.execute(update);
    assertTrue(updateResult.getIndexUpdates() > 0);

    final KeyQuery get = QueryBuilder.query(entity.getKey());
    final QueryResult getResult = datastore.execute(get);

    assertEquals(40, getResult.getEntity().getInteger("age").intValue());
  }

  @Test
  public void testUpdateRemoveProperty() throws Exception {
    final Entity existing = Entity.builder("employee")
        .property("fullname", "Fred Blinge")
        .property("age", 40)
        .build();
    final MutationResult result = datastore.execute(QueryBuilder.insert(existing));
    final Key existingKey = result.getInsertKey();

    final Entity entity = Entity.builder(existing)
        .key(existingKey)
        .remove("age")
        .build();

    final Update update = QueryBuilder.update(entity);
    final MutationResult updateResult = datastore.execute(update);
    assertTrue(updateResult.getIndexUpdates() > 0);

    final KeyQuery get = QueryBuilder.query(entity.getKey());
    final QueryResult getResult = datastore.execute(get);

    assertNull(getResult.getEntity().getInteger("age"));
  }
}
