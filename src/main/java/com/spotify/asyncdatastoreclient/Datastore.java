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

import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;
import java.util.List;

/**
 * The Datastore class encapsulates the Cloud Datastore API and handles
 * calling the datastore backend.
 * <p>
 * To create a Datastore object, call the static method {@code Datastore.create()}
 * passing configuration. A scheduled task will begin that automatically refreshes
 * the API access token for you.
 * <p>
 * Call {@code close()} to perform all necessary clean up.
 */
public interface Datastore extends Closeable {

  static Datastore create(final DatastoreConfig config) {
    return new DatastoreImpl(config);
  }

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the
   * request is successful.
   *
   * @return the result of the transaction request.
   */
  TransactionResult transaction() throws DatastoreException;

  /**
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the
   * request is successful.
   *
   * @return the result of the transaction request.
   */
  ListenableFuture<TransactionResult> transactionAsync();

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn the transaction.
   * @return the result of the rollback request.
   */
  RollbackResult rollback(final TransactionResult txn) throws DatastoreException;

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn the transaction.
   * @return the result of the rollback request.
   */
  ListenableFuture<RollbackResult> rollbackAsync(final ListenableFuture<TransactionResult> txn);

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only
   * operations without mutations.
   *
   * @param txn the transaction.
   * @return the result of the commit request.
   */
  MutationResult commit(final TransactionResult txn) throws DatastoreException;

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only
   * operations without mutations.
   *
   * @param txn the transaction.
   * @return the result of the commit request.
   */
  ListenableFuture<MutationResult> commitAsync(final ListenableFuture<TransactionResult> txn);

  /**
   * Execute a allocate ids statement.
   *
   * @param statement the statement to execute.
   * @return the result of the allocate ids request.
   */
  AllocateIdsResult execute(final AllocateIds statement) throws DatastoreException;

  /**
   * Execute a allocate ids statement.
   *
   * @param statement the statement to execute.
   * @return the result of the allocate ids request.
   */
  ListenableFuture<AllocateIdsResult> executeAsync(final AllocateIds statement);

  /**
   * Execute a keyed query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  QueryResult execute(final KeyQuery statement) throws DatastoreException;

  /**
   * Execute a multi-keyed query statement.
   *
   * @param statements the statements to execute.
   * @return the result of the query request.
   */
  QueryResult execute(final List<KeyQuery> statements) throws DatastoreException;

  /**
   * Execute a keyed query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final KeyQuery statement);

  /**
   * Execute a multi-keyed query statement.
   *
   * @param statements the statements to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final List<KeyQuery> statements);

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(final KeyQuery statement, final TransactionResult txn) throws DatastoreException;

  /**
   * Execute a multi-keyed query statement in a given transaction.
   *
   * @param statements the statements to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(final List<KeyQuery> statements, final TransactionResult txn) throws DatastoreException;

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final KeyQuery statement, final ListenableFuture<TransactionResult> txn);

  /**
   * Execute a multi-keyed query statement in a given transaction.
   *
   * @param statements the statements to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final List<KeyQuery> statements, final ListenableFuture<TransactionResult> txn);

  /**
   * Execute a mutation query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the mutation request.
   */
  MutationResult execute(final MutationStatement statement) throws DatastoreException;

  /**
   * Execute a mutation query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(final MutationStatement statement);

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  MutationResult execute(final MutationStatement statement, final TransactionResult txn) throws DatastoreException;

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(final MutationStatement statement, final ListenableFuture<TransactionResult> txn);

  /**
   * Execute a batch mutation query statement.
   *
   * @param batch to execute.
   * @return the result of the mutation request.
   */
  MutationResult execute(final Batch batch) throws DatastoreException;

  /**
   * Execute a batch mutation query statement.
   *
   * @param batch to execute.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(final Batch batch);

  /**
   * Execute a batch mutation query statement in a given transaction.
   *
   * @param batch to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  MutationResult execute(final Batch batch, final TransactionResult txn) throws DatastoreException;

  /**
   * Execute a batch mutation query statement in a given transaction.
   *
   * @param batch to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(final Batch batch, final ListenableFuture<TransactionResult> txn);

  /**
   * Execute a query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  QueryResult execute(final Query statement) throws DatastoreException;

  /**
   * Execute a query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final Query statement);

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(final Query statement, final TransactionResult txn) throws DatastoreException;

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(final Query statement, final ListenableFuture<TransactionResult> txn);
}
