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

import java.util.List;
import java.util.stream.Collectors;

/**
 * A query result.
 *
 * Returned from all mutation operations.
 */
public class MutationResult implements Result {

  private final DatastoreV1.MutationResult result;

  private MutationResult(final DatastoreV1.MutationResult result) {
    this.result = result;
  }

  static MutationResult build(final DatastoreV1.CommitResponse response) {
    return response.hasMutationResult() ? new MutationResult(response.getMutationResult()) : build();
  }

  /**
   * Build an empty result.
   *
   * @return a new empty mutation result.
   */
  public static MutationResult build() {
    return new MutationResult(DatastoreV1.MutationResult.getDefaultInstance());
  }

  /**
   * Return the first entity key that was inserted or null if empty.
   *
   * This is a shortcut for {@code getInsertKeys().get(0)}
   *
   * @return a key that describes the newly inserted entity.
   */
  public Key getInsertKey() {
    if (result.getInsertAutoIdKeyCount() == 0) {
      return null;
    }
    return Key.builder(result.getInsertAutoIdKey(0)).build();
  }

  /**
   * Return all entity keys that were inserted for automatically generated
   * key ids.
   *
   * @return a list of keys that describe the newly inserted entities.
   */
  public List<Key> getInsertKeys() {
    return ImmutableList.copyOf(result.getInsertAutoIdKeyList().stream()
        .map(key -> Key.builder(key).build())
        .collect(Collectors.toList()));
  }

  /**
   * Return the number of indexes updated during the mutation operation.
   *
   * @return the number of index updates.
   */
  public int getIndexUpdates() {
    return result.getIndexUpdates();
  }
}
