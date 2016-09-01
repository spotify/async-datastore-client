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

import com.google.datastore.v1.Mutation;

import java.util.List;

/**
 * An insert statement.
 *
 * Insert a single entity using a given {@code Key} and properties.
 */
public class Insert extends KeyedStatement implements MutationStatement {

  protected Entity.Builder entity;

  Insert(final Key key) {
    super(key);
    this.entity = Entity.builder(key);
  }

  Insert(final Entity entity) {
    super(entity.getKey());
    this.entity = Entity.builder(entity);
  }

  /**
   * Adds a property name and value to the entity to before inserting.
   *
   * @param name the property name.
   * @param value the property {@code Value}.
   * @return this insert statement.
   */
  public Insert value(final String name, final Object value) {
    entity.property(name, value);
    return this;
  }

  /**
   * Adds a property name and value to the entity before inserting.
   *
   * @param name the property name.
   * @param value the property {@code Value}.
   * @param indexed indicates whether the {@code Value} should be indexed or not.
   * @return this insert statement.
   */
  public Insert value(final String name, final Object value, final boolean indexed) {
    entity.property(name, value, indexed);
    return this;
  }

  /**
   * Adds a property name and list of values to the entity before inserting.
   *
   * @param name the property name.
   * @param values a list of property {@code Value}s.
   * @return this insert statement.
   */
  public Insert value(final String name, final List<Object> values) {
    entity.property(name, values);
    return this;
  }

  @Override
  public Mutation getPb(final String namespace) {
    final com.google.datastore.v1.Mutation.Builder mutation =
      com.google.datastore.v1.Mutation.newBuilder();

    return mutation.setInsert(entity.build().getPb(namespace)).build();
  }
}
