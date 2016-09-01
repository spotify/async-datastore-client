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
 * An update statement.
 *
 * Update a single entity based on a given {@code Key} and properties.
 */
public class Update extends KeyedStatement implements MutationStatement {

  private boolean upsert;

  protected Entity.Builder entity;

  Update(final Key key) {
    super(key);
    this.entity = Entity.builder(key);
  }

  Update(final Entity entity) {
    super(entity.getKey());
    this.entity = Entity.builder(entity);
  }

  /**
   * Adds a property name and value to the entity to before updating.
   *
   * @param name the property name.
   * @param value the property {@code Value}.
   * @return this update statement.
   */
  public Update value(final String name, final Object value) {
    entity.property(name, value);
    return this;
  }

  /**
   * Adds a property name and value to the entity before updating.
   *
   * @param name the property name.
   * @param value the property {@code Value}.
   * @param indexed indicates whether the {@code Value} should be indexed or not.
   * @return this update statement.
   */
  public Update value(final String name, final Object value, final boolean indexed) {
    entity.property(name, value, indexed);
    return this;
  }

  /**
   * Adds a property name and list of values to the entity before updating.
   *
   * @param name the property name.
   * @param values a list of property {@code Value}s.
   * @return this update statement.
   */
  public Update value(final String name, final List<Object> values) {
    entity.property(name, values);
    return this;
  }

  /**
   * Adds a property name and list of values to the entity before updating.
   *
   * @param name the property name.
   * @param values a list of property {@code Value}s.
   * @param indexed indicates whether the {@code Value}s should be indexed or not.
   * @return this update statement.
   */
  public Update value(final String name, final List<Object> values, final boolean indexed) {
    entity.property(name, values, indexed);
    return this;
  }

  /**
   * Indicates whether this should be an "upsert" operation. An upsert will
   * add the enitiy if is does not exist, whereas a regualar update will fail
   * if the entity does not exist.
   *
   * @return this update statement.
   */
  public Update upsert() {
    this.upsert = true;
    return this;
  }

  @Override
  public Mutation getPb(final String namespace) {
    final com.google.datastore.v1.Mutation.Builder mutation =
      com.google.datastore.v1.Mutation.newBuilder();
    if (upsert) {
      mutation.setUpsert(entity.build().getPb(namespace));
    } else {
      mutation.setUpdate(entity.build().getPb(namespace));
    }
    return mutation.build();
  }
}
