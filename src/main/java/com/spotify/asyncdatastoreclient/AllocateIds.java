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

import com.google.api.client.util.Lists;

import java.util.List;
import java.util.stream.Collectors;

/**
 * An allocate ids statement.
 *
 * A allocate ids operation sent to Datastore.
 */
public class AllocateIds implements Statement {

  private final List<Key> keys;

  AllocateIds() {
    this.keys = Lists.newArrayList();
  }

  AllocateIds(final List<Key> keys) {
    this.keys = keys;
  }

  /**
   * Adds a partial key for allocation.
   *
   * @param kind the partial key kind.
   * @return this allocate ids statement.
   */
  public AllocateIds add(final String kind) {
    this.keys.add(Key.builder(kind).build());
    return this;
  }

  /**
   * Adds a partial key for allocation.
   *
   * @param key the partial key.
   * @return this allocate ids statement.
   */
  public AllocateIds add(final Key key) {
    this.keys.add(key);
    return this;
  }

  List<com.google.datastore.v1.Key> getPb(final String namespace) {
    return keys.stream()
        .map(key -> Key.builder(key).build().getPb(namespace))
        .collect(Collectors.toList());
  }

}
