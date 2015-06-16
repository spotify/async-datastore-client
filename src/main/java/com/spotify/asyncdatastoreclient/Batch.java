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
import com.google.api.services.datastore.DatastoreV1;

import java.util.List;

/**
 * A batch mutation statement.
 *
 * Will batch a collection of mutation operations into a single operation.
 */
public class Batch implements MutationStatement {

  private final List<MutationStatement> statements = Lists.newArrayList();

  /**
   * Adds a statement to this batch.
   *
   * @param statement the mutation statement to add.
   * @return this batch statement.
   */
  public Batch add(final MutationStatement statement) {
    statements.add(statement);
    return this;
  }

  @Override
  public DatastoreV1.Mutation getPb(final String namespace) {
    final DatastoreV1.Mutation.Builder mutation = DatastoreV1.Mutation.newBuilder();
    for (final MutationStatement statement : statements) {
      mutation.mergeFrom(statement.getPb(namespace));
    }
    return mutation.build();
  }
}
