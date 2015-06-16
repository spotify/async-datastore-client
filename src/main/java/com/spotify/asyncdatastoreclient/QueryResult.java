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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A query result.
 *
 * Returned from query operations.
 */
public class QueryResult implements Result, Iterable<Entity> {

  private final List<Entity> entities;
  private final ByteString cursor;

  private QueryResult(final List<Entity> entities) {
    this.entities = entities;
    this.cursor = null;
  }

  private QueryResult(final List<Entity> entities, final ByteString cursor) {
    this.entities = entities;
    this.cursor = cursor;
  }

  static QueryResult build(final DatastoreV1.LookupResponse response) {
    return new QueryResult(ImmutableList.copyOf(
        response.getFoundList().stream()
            .map(entity -> Entity.builder(entity.getEntity()).build())
            .collect(Collectors.toList())));
  }

  static QueryResult build(final DatastoreV1.RunQueryResponse response) {
    final DatastoreV1.QueryResultBatch batch = response.getBatch();
    return new QueryResult(ImmutableList.copyOf(
        batch.getEntityResultList().stream()
            .map(entity -> Entity.builder(entity.getEntity()).build())
            .collect(Collectors.toList())),
                           batch.hasEndCursor() ? batch.getEndCursor() : null);
  }

  /**
   * Build an empty result.
   *
   * @return a new empty query result.
   */
  public static QueryResult build() {
    return new QueryResult(ImmutableList.of());
  }

  /**
   * Return the first entity returned from the query or null if not found.
   *
   * This is a shortcut for {@code getAll().get(0)}
   *
   * @return an entity returned from the Datastore.
   */
  public Entity getEntity() {
    return Iterables.getFirst(entities, null);
  }

  /**
   * Return all entities returned from the query.
   *
   * @return a list of entities returned from the query.
   */
  public List<Entity> getAll() {
    return entities;
  }

  /**
   * An iterator for all entities returned from the query.
   *
   * @return an entity iterator.
   */
  @Override
  public Iterator<Entity> iterator() {
    return entities.iterator();
  }

  /**
   * The last cursor position after returning all entities for this batch.
   *
   * This is useful for paging; if you supply a {@code limit()}, then the
   * cursor can be used when retrieving the next batch: {@code QueryfromCursor()}.
   *
   * @return the last cursor position.
   */
  public ByteString getCursor() {
    return cursor;
  }
}
