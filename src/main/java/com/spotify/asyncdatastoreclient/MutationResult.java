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

import java.util.List;
import java.util.stream.Collectors;

/**
 * A query result.
 *
 * Returned from all mutation operations.
 */
public final class MutationResult implements Result {

  private final List<com.google.datastore.v1.MutationResult> result;
  private final int indexUpdates;

  private MutationResult(final com.google.datastore.v1.MutationResult result, int indexUpdates) {
    this.result = ImmutableList.of(result);
    this.indexUpdates = indexUpdates;
  }

  private MutationResult(final List<com.google.datastore.v1.MutationResult> results, int indexUpdates) {
    this.result = ImmutableList.copyOf(results);
    this.indexUpdates = indexUpdates;
  }

  static MutationResult build(final com.google.datastore.v1.CommitResponse response) {
    return new MutationResult(response.getMutationResultsList(), response.getIndexUpdates());
  }

  /**
   * Build an empty result.
   *
   * @return a new empty mutation result.
   */
  public static MutationResult build() {
    return new MutationResult(com.google.datastore.v1.MutationResult.getDefaultInstance(), 0);
  }

  /**
   * Return the first entity key that was inserted or null if empty.
   *
   * This is a shortcut for {@code getInsertKeys().get(0)}
   *
   * @return a key that describes the newly inserted entity.
   */
  public Key getInsertKey() {
    if (result.isEmpty()) {
      return null;
    }
    return Key.builder(result.get(0).getKey()).build();
  }

  /**
   * Return all entity keys that were inserted for automatically generated
   * key ids.
   *
   * @return a list of keys that describe the newly inserted entities.
   */
  public List<Key> getInsertKeys() {
    return ImmutableList.copyOf(result.stream()
        .map(r -> Key.builder(r.getKey()).build())
        .collect(Collectors.toList()));
  }

  /**
   * Return the number of indexes updated during the mutation operation.
   *
   * @return the number of index updates.
   */
  public int getIndexUpdates() {
    return indexUpdates;
  }
}
