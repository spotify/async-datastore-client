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
import com.google.protobuf.ByteString;

/**
 * A transaction result.
 *
 * Returned from a transaction operation.
 */
public final class TransactionResult implements Result {

  private final ByteString transaction;

  private TransactionResult(final ByteString transaction) {
    this.transaction = transaction;
  }

  static TransactionResult build(final DatastoreV1.BeginTransactionResponse response) {
    return response.hasTransaction() ? new TransactionResult(response.getTransaction()) : build();
  }

  /**
   * Build an empty result.
   *
   * @return a new empty transaction result.
   */
  public static TransactionResult build() {
    return new TransactionResult(null);
  }

  ByteString getTransaction() {
    return transaction;
  }
}
