package com.spotify.asyncdatastoreclient;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

/**
 * Created by dxia on 6/23/16.
 */
public class FakeDatastore implements DatastoreInterface {

  @Override
  public TransactionResult transaction() throws DatastoreException {
    return null;
  }

  @Override
  public TransactionResult transaction(IsolationLevel isolationLevel)
      throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<TransactionResult> transactionAsync() {
    return null;
  }

  @Override
  public ListenableFuture<TransactionResult> transactionAsync(
      IsolationLevel isolationLevel) {
    return null;
  }

  @Override
  public RollbackResult rollback(TransactionResult txn) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<RollbackResult> rollbackAsync(ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public MutationResult commit(TransactionResult txn) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<MutationResult> commitAsync(ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public AllocateIdsResult execute(AllocateIds statement) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<AllocateIdsResult> executeAsync(AllocateIds statement) {
    return null;
  }

  @Override
  public QueryResult execute(KeyQuery statement) throws DatastoreException {
    return null;
  }

  @Override
  public QueryResult execute(List<KeyQuery> statements) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(KeyQuery statement) {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(List<KeyQuery> statements) {
    return null;
  }

  @Override
  public QueryResult execute(KeyQuery statement, TransactionResult txn) throws DatastoreException {
    return null;
  }

  @Override
  public QueryResult execute(List<KeyQuery> statements, TransactionResult txn)
      throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(KeyQuery statement,
                                                    ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(List<KeyQuery> statements,
                                                    ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public MutationResult execute(MutationStatement statement) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(MutationStatement statement) {
    return null;
  }

  @Override
  public MutationResult execute(MutationStatement statement, TransactionResult txn)
      throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(MutationStatement statement,
                                                       ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public QueryResult execute(Query statement) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(Query statement) {
    return null;
  }

  @Override
  public QueryResult execute(Query statement, TransactionResult txn) throws DatastoreException {
    return null;
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(Query statement,
                                                    ListenableFuture<TransactionResult> txn) {
    return null;
  }

  @Override
  public void close() throws Exception {

  }
}
