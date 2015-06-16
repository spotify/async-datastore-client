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

import java.util.List;

/**
 * Static methods to build a Datastore query.
 * <p>
 * The provided builders perform very little validation of the built query.
 * Therefore there is no guarantee that a built query is valid, and it is
 * definitively possible to create invalid queries.
 * <p>
 * Note that it could be convenient to use an 'import static' to use the
 * methods of this class.
 */
public class QueryBuilder {

  /**
   * Start building a new INSERT query.
   *
   * @param kind the kind of entity to insert.
   * @return an in-construction INSERT query.
   */
  public static Insert insert(final String kind) {
    return new Insert(Key.builder(kind).build());
  }

  /**
   * Start building a new INSERT query.
   *
   * @param kind the kind of entity to insert.
   * @param id the key id of this entity to insert.
   * @return an in-construction INSERT query.
   */
  public static Insert insert(final String kind, final long id) {
    return new Insert(Key.builder(kind, id).build());
  }

  /**
   * Start building a new INSERT query.
   *
   * @param kind the kind of entity to insert.
   * @param name the key name of this entity to insert.
   * @return an in-construction INSERT query.
   */
  public static Insert insert(final String kind, final String name) {
    return new Insert(Key.builder(kind, name).build());
  }

  /**
   * Start building a new INSERT query.
   *
   * @param key the key of this entity to insert.
   * @return an in-construction INSERT query.
   */
  public static Insert insert(final Key key) {
    return new Insert(key);
  }

  /**
   * Start building a new INSERT query.
   *
   * @param entity the entity to insert.
   * @return an in-construction INSERT query.
   */
  public static Insert insert(final Entity entity) {
    return new Insert(entity);
  }

  /**
   * Start building a new UPDATE query.
   *
   * @param kind the kind of entity to update.
   * @return an in-construction UPDATE query.
   */
  public static Update update(final String kind) {
    return new Update(Key.builder().path(kind).build());
  }

  /**
   * Start building a new UPDATE query.
   *
   * @param kind the kind of entity to update.
   * @param id the key id of this entity to update.
   * @return an in-construction UPDATE query.
   */
  public static Update update(final String kind, final long id) {
    return new Update(Key.builder().path(kind, id).build());
  }

  /**
   * Start building a new UPDATE query.
   *
   * @param kind the kind of entity to update.
   * @param name the key name of the entity to update.
   * @return an in-construction UPDATE query.
   */
  public static Update update(final String kind, final String name) {
    return new Update(Key.builder().path(kind, name).build());
  }

  /**
   * Start building a new UPDATE query.
   *
   * @param key the key of the entity to update.
   * @return an in-construction UPDATE query.
   */
  public static Update update(final Key key) {
    return new Update(key);
  }

  /**
   * Start building a new UPDATE query.
   *
   * @param entity the entity to update.
   * @return an in-construction UPDATE query.
   */
  public static Update update(final Entity entity) {
    return new Update(entity);
  }

  /**
   * Start building a new DELETE query.
   *
   * @param kind the kind of entity to delete.
   * @param id the key id of this entity to delete.
   * @return an in-construction DELETE query.
   */
  public static Delete delete(final String kind, final long id) {
    return new Delete(Key.builder().path(kind, id).build());
  }

  /**
   * Start building a new DELETE query.
   *
   * @param kind the kind of entity to delete.
   * @param name the key name of the entity to delete.
   * @return an in-construction DELETE query.
   */
  public static Delete delete(final String kind, final String name) {
    return new Delete(Key.builder().path(kind, name).build());
  }

  /**
   * Start building a new DELETE query.
   *
   * @param key the key of the entity to delete.
   * @return an in-construction DELETE query.
   */
  public static Delete delete(final Key key) {
    return new Delete(key);
  }

  /**
   * Start building a new ALLOCATE query.
   *
   * @param keys list of partial keys to allocate.
   * @return an in-construction ALLOCATE query.
   */
  public static AllocateIds allocate(final List<Key> keys) {
    return new AllocateIds(keys);
  }

  /**
   * Start building a new ALLOCATE query.
   *
   * @return an in-construction ALLOCATE query.
   */
  public static AllocateIds allocate() {
    return new AllocateIds();
  }

  /**
   * Start building a new BATCH query.
   * <p>
   * This method will build a query for batching operations into a single
   * call to Datastore.
   * <p>
   * NOTE: Use a transaction to make this batch operation atomic, otherwise
   * and error might mean that some, none, or all of the requested operations
   * have been performed.
   *
   * @return an in-construction BATCH query.
   */
  public static Batch batch() {
    return new Batch();
  }

  /**
   * Start building a new KEYQUERY query.
   * <p>
   * You should use a {@code KeyQuery} to retrieve a single entity based
   * on its key.
   *
   * @param kind the kind of entity to retrieve.
   * @param id the key id of this entity to retrieve.
   * @return an in-construction KEYQUERY query.
   */
  public static KeyQuery query(final String kind, final long id) {
    return new KeyQuery(Key.builder().path(kind, id).build());
  }

  /**
   * Start building a new KEYQUERY query.
   * <p>
   * You should use a {@code KeyQuery} to retrieve a single entity based
   * on its key.
   *
   * @param kind the kind of entity to retrieve.
   * @param name the key name of this entity to retrieve.
   * @return an in-construction KEYQUERY query.
   */
  public static KeyQuery query(final String kind, final String name) {
    return new KeyQuery(Key.builder().path(kind, name).build());
  }

  /**
   * Start building a new KEYQUERY query.
   * <p>
   * Use a {@code KeyQuery} to retrieve a single entity based on its key.
   *
   * @param key the key of entity to retrieve.
   * @return an in-construction KEYQUERY query.
   */
  public static KeyQuery query(final Key key) {
    return new KeyQuery(key);
  }

  /**
   * Start building a new QUERY query.
   * <p>
   * Use a {@code Query} to retrieve one or more entities that satisfy a
   * given criteria and order.
   *
   * @return an in-construction QUERY query.
   */
  public static Query query() {
    return new Query();
  }

  /**
   * Creates an "equal" {@code Filter} stating the provided property
   * must be equal to a given value.
   *
   * @param name the property name.
   * @param value the value.
   * @return a query filter.
   */
  public static Filter eq(final String name, final Object value) {
    return new Filter(name, Filter.Operator.EQUAL, Value.builder().value(value).build());
  }

  /**
   * Creates an "less than" {@code Filter} stating the provided property
   * must be less than a given value.
   *
   * @param name the property name.
   * @param value the value.
   * @return a query filter.
   */
  public static Filter lt(final String name, final Object value) {
    return new Filter(name, Filter.Operator.LESS_THAN, Value.builder().value(value).build());
  }

  /**
   * Creates an "less than or equal" {@code Filter} stating the provided
   * property must be less than or equal to a given value.
   *
   * @param name the property name.
   * @param value the value.
   * @return a query filter.
   */
  public static Filter lte(final String name, final Object value) {
    return new Filter(name, Filter.Operator.LESS_THAN_OR_EQUAL, Value.builder().value(value).build());
  }

  /**
   * Creates an "greater than" {@code Filter} stating the provided property
   * must be greater than a given value.
   *
   * @param name the property name.
   * @param value the value.
   * @return a query filter.
   */
  public static Filter gt(final String name, final Object value) {
    return new Filter(name, Filter.Operator.GREATER_THAN, Value.builder().value(value).build());
  }

  /**
   * Creates an "greater than or equal" {@code Filter} stating the provided
   * property must be greater than or equal to a given value.
   *
   * @param name the property name.
   * @param value the value.
   * @return a query filter.
   */
  public static Filter gte(final String name, final Object value) {
    return new Filter(name, Filter.Operator.GREATER_THAN_OR_EQUAL, Value.builder().value(value).build());
  }

  /**
   * Creates an "ancestor" {@code Filter} stating the provided key must be
   * an ancestor of the queried entities.
   *
   * @param kind the ancestor kind.
   * @param id the ancestor key id.
   * @return a query filter.
   */
  public static Filter ancestor(final String kind, final long id) {
    return new Filter("__key__", Filter.Operator.HAS_ANCESTOR, Value.builder().value(Key.builder(kind, id).build()).build());
  }

  /**
   * Creates an "ancestor" {@code Filter} stating the provided key must be
   * an ancestor of the queried entities.
   *
   * @param kind the ancestor kind.
   * @param name the ancestor key name.
   * @return a query filter.
   */
  public static Filter ancestor(final String kind, final String name) {
    return new Filter("__key__", Filter.Operator.HAS_ANCESTOR, Value.builder().value(Key.builder(kind, name).build()).build());
  }

  /**
   * Creates an "ancestor" {@code Filter} stating the provided key must be
   * an ancestor of the queried entities.
   *
   * @param key the ancestor key.
   * @return a query filter.
   */
  public static Filter ancestor(final Key key) {
    return new Filter("__key__", Filter.Operator.HAS_ANCESTOR, Value.builder().value(key).build());
  }

  /**
   * Ascending ordering for the provided property name.
   *
   * @param name the property name.
   * @return the query ordering.
   */
  public static Order asc(final String name) {
    return new Order(name, Order.Direction.ASCENDING);
  }

  /**
   * Descending ordering for the provided property name.
   *
   * @param name the property name.
   * @return the query ordering.
   */
  public static Order desc(final String name) {
    return new Order(name, Order.Direction.DESCENDING);
  }

  /**
   * Creates a {@code Group} stating the query should be grouped
   * by the provided property name.
   *
   * @param name the name of the property to group by.
   * @return a query grouping.
   */
  public static Group group(final String name) {
    return new Group(name);
  }
}
