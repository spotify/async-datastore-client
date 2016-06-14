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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import org.junit.Test;

import java.util.Date;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class EntityTest {

  @Test
  public void testEntityBuilder() throws Exception {
    final Entity entity1 = Entity.builder("employee").build();
    assertEquals("employee", entity1.getKey().getKind());

    final Entity entity2 = Entity.builder("employee", "fred").build();
    assertEquals("fred", entity2.getKey().getName());

    final Entity entity3 = Entity.builder("employee", 1234567L).build();
    assertEquals(1234567L, entity3.getKey().getId().longValue());

    final Key test4 = Key.builder("employee", 1234567L).build();
    final Entity entity4 = Entity.builder(test4).build();
    assertEquals("employee", entity4.getKey().getKind());
    assertEquals(1234567L, entity4.getKey().getId().longValue());

    final Date now = new Date();
    final ByteString picture = ByteString.copyFrom(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9});
    final Entity address = Entity.builder("address", 222222L)
        .property("first_line", "22 Arcadia Ave")
        .property("zipcode", "90210").build();

    final Entity entity5 = Entity.builder("employee", 1234567L)
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

    assertEquals("employee", entity5.getKey().getKind());
    assertEquals(1234567L, entity5.getKey().getId().longValue());
    assertEquals("Fred Blinge", entity5.getString("fullname"));
    assertEquals("Freddie", entity5.getString("nickname"));
    assertEquals(2.43, entity5.getDouble("height"), 0.01);
    assertEquals(22.5, entity5.getDouble("holiday_allowance"), 0.01);
    assertEquals(123456789, entity5.getInteger("payroll_number").intValue());
    assertEquals(40, entity5.getInteger("age").intValue());
    assertFalse(entity5.getBoolean("senior_role"));
    assertTrue(entity5.getBoolean("active"));
    assertEquals(now, entity5.getDate("start_date"));
    assertEquals(now, entity5.getDate("update_date"));
    assertTrue(picture.equals(entity5.getBlob("picture")));
    assertEquals(now, entity5.getDate("update_date"));
    assertEquals("address", entity5.getEntity("address").getKey().getKind());
    assertEquals(222222L, entity5.getEntity("address").getKey().getId().longValue());
    assertEquals("22 Arcadia Ave", entity5.getEntity("address").getString("first_line"));
    assertEquals("90210", entity5.getEntity("address").getString("zipcode"));
    assertEquals("employee", entity5.getKey("manager").getKind());
    assertEquals(234567L, entity5.getKey("manager").getId().longValue());
    assertTrue(Iterables.elementsEqual(entity5.getList("workdays", String.class), ImmutableList.of(
        "Monday", "Tuesday", "Friday")));
    assertTrue(Iterables.elementsEqual(entity5.getList("workdays"), ImmutableList.of(
        Value.from("Monday").build(), Value.from("Tuesday").build(), Value.from("Friday").build())));
    assertTrue(Iterables.elementsEqual(entity5.getList("overtime_hours", Long.class), ImmutableList.of(2L, 3L, 4L)));
    assertTrue(entity5.contains("picture"));
    assertTrue(entity5.toString().contains("active:true"));
    assertTrue(entity5.toString().contains("first_line:22 Arcadia Ave"));
    assertTrue(entity5.toString().contains("height:2.43"));
    assertTrue(entity5.toString().contains("overtime_hours:[2, 3, 4]"));
  }

  @Test
  public void testRemoveProperty() throws Exception {
    final Entity.Builder builder = Entity.builder("employee", 1234567L)
        .property("fullname", "Fred Blinge")
        .property("nickname", "Freddie", false)
        .property("height", 2.43);
    builder.remove("nickname");
    final Entity entity = builder.build();

    assertEquals("Fred Blinge", entity.getString("fullname"));
    assertNull(entity.getString("nickname"));
    assertEquals(2.43, entity.getDouble("height"), 0.01);
  }

  @Test
  public void testGetEmptyProperty() throws Exception {
    final Entity entity = Entity.builder().property("bar", Value.from("value").build()).build();

    assertEquals(Optional.empty(), entity.get("foo"));
    assertEquals(Optional.of("value"), entity.get("bar").map(Value::getString));
  }
}
