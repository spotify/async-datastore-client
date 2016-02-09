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

import com.google.api.services.datastore.DatastoreV1;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class KeyTest {

  @Test
  public void testKeyBuilder() throws Exception {
    final Key test1 = Key.builder("employee").build();
    assertEquals("employee", test1.getPath().get(0).getKind());

    final Key test2 = Key.builder("employee", "fred").build();
    assertEquals("fred", test2.getPath().get(0).getName());

    final Key test3 = Key.builder("employee", 1234567L).build();
    assertEquals(1234567L, test3.getPath().get(0).getId().longValue());

    final Key test4 = Key.builder("employee", 1234567L).build();
    assertNull(test4.getPath().get(0).getName());

    final Key test5 = Key.builder("employee", "fred").build();
    assertNull(test5.getPath().get(0).getId());

    final Key test6 = Key.builder("employee", "jack", test5).build();
    assertEquals("fred", test6.getPath().get(0).getName());
    assertEquals("jack", test6.getPath().get(1).getName());

    final Key test7 = Key.builder("employee", test5).build();
    assertEquals("fred", test7.getPath().get(0).getName());
    assertFalse(test7.isComplete());

    final Key test8 = Key.builder().path("employee").build();
    assertEquals("employee", test8.getPath().get(0).getKind());

    final Key test9 = Key.builder().path("employee", "fred").build();
    assertEquals("fred", test9.getPath().get(0).getName());

    final Key test10 = Key.builder().path("employee", 1234567L).build();
    assertEquals(1234567L, test10.getPath().get(0).getId().longValue());

    final Key test11 = Key.builder().parent(test9).path("employee", 1234567L).build();
    assertEquals("fred", test11.getPath().get(0).getName());
    assertEquals(1234567L, test11.getPath().get(1).getId().longValue());

    final Key test12 = Key.builder().path("employee", "fred").namespace("test").build();
    assertEquals("test", test12.getNamespace());

    final Key test13 = Key.builder().path("employee", "fred").build();
    assertTrue(test13.isComplete());

    final Key test14 = Key.builder().path("employee", 1234567L).build();
    assertTrue(test14.isComplete());

    final Key test15 = Key.builder().path("employee").build();
    assertFalse(test15.isComplete());

    final Key test16 = Key.builder(test13).build();
    assertEquals("employee", test16.getPath().get(0).getKind());
    assertEquals("fred", test16.getPath().get(0).getName());
    assertTrue(test16.isComplete());

    final DatastoreV1.Key test17Pb = DatastoreV1.Key.newBuilder()
        .addPathElement(DatastoreV1.Key.PathElement.newBuilder().setKind("employee").setName("fred")).build();
    final Key test17 = Key.builder(test17Pb).build();
    assertEquals("employee", test17.getPath().get(0).getKind());
    assertEquals("fred", test17.getPath().get(0).getName());
    assertTrue(test17.isComplete());
  }

  @Test
  public void testKeyPath() throws Exception {
    final Key test1 = Key.builder().path("employee", "fred").path("employee", "jack").build();
    assertEquals("fred", test1.getPath().get(0).getName());
    assertEquals("jack", test1.getPath().get(1).getName());

    final Key test2 = Key.builder().path("employee", "fred").path("employee").build();
    assertFalse(test2.isComplete());
    final List<Key.Element> path = test2.getPath();
    assertEquals(2, path.size());
    assertEquals("fred", path.get(0).getName());
    assertNull(path.get(1).getName());

    final Key test3 = Key.builder().parent(test1).path("employee", "peter").build();
    assertEquals("fred", test3.getPath().get(0).getName());
    assertEquals("jack", test3.getPath().get(1).getName());
    assertEquals("peter", test3.getPath().get(2).getName());

    final Key test4 = Key.builder().path("employee", "fred").path("employee", "jack").build();
    assertEquals("{employee:fred, employee:jack}", test4.toString());

    final Key test5 = Key.builder("employee", "fred").path("employee").build();
    assertEquals("{employee:fred, employee:null}", test5.toString());
  }

  @Test
  public void testKeyElement() throws Exception {
    final Key test1 = Key.builder().path("employee", "fred").path("employee", "jack").build();
    assertEquals("employee", test1.getKind());
    assertEquals("jack", test1.getName());
    assertNull(test1.getId());

    final Key test2 = Key.builder().path("employee", "fred").path("employee", 1234567L).build();
    assertEquals("employee", test2.getKind());
    assertEquals(1234567L, test2.getId().longValue());
    assertNull(test2.getName());

    final Key test3 = Key.builder().path("employee", "fred").path("employee").build();
    assertNull(test3.getName());
    assertNull(test3.getId());

    final Key test4 = Key.builder().build();
    assertNull(test4.getKind());
    assertNull(test4.getName());
    assertNull(test4.getId());
  }

  @Test
  public void testGetPb() throws Exception {
    final Key test1 = Key.builder().build();
    assertEquals("namespace", test1.getPb("namespace").getPartitionId().getNamespace());

    // Passing in null should not throw exception
    assertNotNull(test1.getPb(null));
  }
}
