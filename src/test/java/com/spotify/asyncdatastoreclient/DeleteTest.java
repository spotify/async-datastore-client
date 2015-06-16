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

@Category(IntegrationTest.class)
public class DeleteTest extends DatastoreTest {

  @Test
  public void testDeleteEntity() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);

    final Delete delete = QueryBuilder.delete("employee", 1234567L);
    final MutationResult result = datastore.execute(delete);
    assertTrue(result.getIndexUpdates() > 0);
  }

  @Test
  public void testDeleteNotExists() throws Exception {
    final Delete delete = QueryBuilder.delete("employee", 1234567L);
    final MutationResult result = datastore.execute(delete);
    assertEquals(0, result.getIndexUpdates());
  }

  @Test
  public void testDeleteByKey() throws Exception {
    final Insert insert = QueryBuilder.insert("employee", 1234567L)
        .value("fullname", "Fred Blinge")
        .value("age", 40, false);
    datastore.execute(insert);

    final Delete delete = QueryBuilder.delete(Key.builder("employee", 1234567L).build());
    final MutationResult result = datastore.execute(delete);
    assertTrue(result.getIndexUpdates() > 0);
  }
}
