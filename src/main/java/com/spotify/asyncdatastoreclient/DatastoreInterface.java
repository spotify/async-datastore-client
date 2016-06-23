package com.spotify.asyncdatastoreclient;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * The Datastore interface encapsulates the Cloud Datastore API and handles
 * calling the datastore backend.
 * <p>
 * Call {@code close()} to perform all necessary clean up.
 */
public interface DatastoreInterface extends AutoCloseable {

  enum IsolationLevel {
    SNAPSHOT,
    SERIALIZABLE
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
   * Start a new transaction with a given isolation level.
   *
   * The returned {@code TransactionResult} contains the transaction if the
   * request is successful.
   *
   * @param isolationLevel the transaction isolation level to request.
   * @return the result of the transaction request.
   */
  TransactionResult transaction(IsolationLevel isolationLevel) throws DatastoreException;

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
   * Start a new transaction.
   *
   * The returned {@code TransactionResult} contains the transaction if the
   * request is successful.
   *
   * @param isolationLevel the transaction isolation level to request.
   * @return the result of the transaction request.
   */
  ListenableFuture<TransactionResult> transactionAsync(IsolationLevel isolationLevel);

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn the transaction.
   * @return the result of the rollback request.
   */
  RollbackResult rollback(TransactionResult txn) throws DatastoreException;

  /**
   * Rollback a given transaction.
   *
   * You normally rollback a transaction in the event of d Datastore failure.
   *
   * @param txn the transaction.
   * @return the result of the rollback request.
   */
  ListenableFuture<RollbackResult> rollbackAsync(ListenableFuture<TransactionResult> txn);

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only
   * operations without mutations.
   *
   * @param txn the transaction.
   * @return the result of the commit request.
   */
  MutationResult commit(TransactionResult txn) throws DatastoreException;

  /**
   * Commit a given transaction.
   *
   * You normally manually commit a transaction after performing read-only
   * operations without mutations.
   *
   * @param txn the transaction.
   * @return the result of the commit request.
   */
  ListenableFuture<MutationResult> commitAsync(ListenableFuture<TransactionResult> txn);

  /**
   * Execute a allocate ids statement.
   *
   * @param statement the statement to execute.
   * @return the result of the allocate ids request.
   */
  AllocateIdsResult execute(AllocateIds statement) throws DatastoreException;

  /**
   * Execute a allocate ids statement.
   *
   * @param statement the statement to execute.
   * @return the result of the allocate ids request.
   */
  ListenableFuture<AllocateIdsResult> executeAsync(AllocateIds statement);

  /**
   * Execute a keyed query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  QueryResult execute(KeyQuery statement) throws DatastoreException;

  /**
   * Execute a multi-keyed query statement.
   *
   * @param statements the statements to execute.
   * @return the result of the query request.
   */
  QueryResult execute(List<KeyQuery> statements) throws DatastoreException;

  /**
   * Execute a keyed query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(KeyQuery statement);

  /**
   * Execute a multi-keyed query statement.
   *
   * @param statements the statements to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(List<KeyQuery> statements);

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(KeyQuery statement, TransactionResult txn) throws DatastoreException;

  /**
   * Execute a multi-keyed query statement in a given transaction.
   *
   * @param statements the statements to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(List<KeyQuery> statements, TransactionResult txn) throws DatastoreException;

  /**
   * Execute a keyed query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(KeyQuery statement, ListenableFuture<TransactionResult> txn);

  /**
   * Execute a multi-keyed query statement in a given transaction.
   *
   * @param statements the statements to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(List<KeyQuery> statements, ListenableFuture<TransactionResult> txn);

  /**
   * Execute a mutation query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the mutation request.
   */
  MutationResult execute(MutationStatement statement) throws DatastoreException;

  /**
   * Execute a mutation query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(MutationStatement statement);

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  MutationResult execute(MutationStatement statement, TransactionResult txn) throws DatastoreException;

  /**
   * Execute a mutation query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the mutation request.
   */
  ListenableFuture<MutationResult> executeAsync(MutationStatement statement, ListenableFuture<TransactionResult> txn);

  /**
   * Execute a query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  QueryResult execute(Query statement) throws DatastoreException;

  /**
   * Execute a query statement.
   *
   * @param statement the statement to execute.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(Query statement);

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  QueryResult execute(Query statement, TransactionResult txn) throws DatastoreException;

  /**
   * Execute a query statement in a given transaction.
   *
   * @param statement the statement to execute.
   * @param txn the transaction to execute the query.
   * @return the result of the query request.
   */
  ListenableFuture<QueryResult> executeAsync(Query statement, ListenableFuture<TransactionResult> txn);
}
