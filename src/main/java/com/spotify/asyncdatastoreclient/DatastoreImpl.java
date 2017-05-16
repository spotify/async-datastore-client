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

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.protobuf.ProtoHttpContent;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.datastore.v1.AllocateIdsRequest;
import com.google.datastore.v1.AllocateIdsResponse;
import com.google.datastore.v1.BeginTransactionRequest;
import com.google.datastore.v1.BeginTransactionResponse;
import com.google.datastore.v1.CommitRequest;
import com.google.datastore.v1.CommitResponse;
import com.google.datastore.v1.LookupRequest;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Mutation;
import com.google.datastore.v1.PartitionId;
import com.google.datastore.v1.ReadOptions;
import com.google.datastore.v1.RollbackRequest;
import com.google.datastore.v1.RollbackResponse;
import com.google.datastore.v1.RunQueryRequest;
import com.google.datastore.v1.RunQueryResponse;
import com.google.protobuf.ByteString;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import com.ning.http.client.extra.ListenableFutureAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * The Datastore implementation.
 */
final class DatastoreImpl implements Datastore {

  private static final Logger log = LoggerFactory.getLogger(Datastore.class);

  private static final String VERSION = "1.0.0";
  private static final String USER_AGENT = "Datastore-Java-Client/" + VERSION + " (gzip)";

  private final DatastoreConfig config;
  private final AsyncHttpClient client;
  private final String prefixUri;

  private final ScheduledExecutorService executor;
  private volatile String accessToken;

  DatastoreImpl(final DatastoreConfig config) {
    this.config = config;
    final AsyncHttpClientConfig httpConfig = new AsyncHttpClientConfig.Builder()
        .setConnectTimeout(config.getConnectTimeout())
        .setRequestTimeout(config.getRequestTimeout())
        .setMaxConnections(config.getMaxConnections())
        .setMaxRequestRetry(config.getRequestRetry())
        .setCompressionEnforced(true)
        .build();

    client = new AsyncHttpClient(httpConfig);
    prefixUri = String.format("%s/%s/projects/%s:", config.getHost(), config.getVersion(), config.getProject());

    executor = Executors.newSingleThreadScheduledExecutor();

    if (config.getCredential() != null) {
      // block while retrieving an access token for the first time
      refreshAccessToken();

      // wake up every 10 seconds to check if access token has expired
      executor.scheduleAtFixedRate(this::refreshAccessToken, 10, 10, TimeUnit.SECONDS);
    }
  }

  @Override
  public void close() {
    executor.shutdown();
    client.close();
  }

  private void refreshAccessToken() {
    final Credential credential = config.getCredential();
    final Long expiresIn = credential.getExpiresInSeconds();

    // trigger refresh if token is about to expire
    if (credential.getAccessToken() == null || expiresIn != null && expiresIn <= 60) {
      try {
        credential.refreshToken();
        final String accessTokenLocal = credential.getAccessToken();
        if (accessTokenLocal != null) {
          this.accessToken = accessTokenLocal;
        }
      } catch (final IOException e) {
        log.error("Storage exception", Throwables.getRootCause(e));
      }
    }
  }

  private static boolean isSuccessful(final int statusCode) {
    return statusCode >= 200 && statusCode < 300;
  }

  private AsyncHttpClient.BoundRequestBuilder prepareRequest(final String method, final ProtoHttpContent payload) throws IOException {
    final AsyncHttpClient.BoundRequestBuilder builder = client.preparePost(prefixUri + method);
    builder.addHeader("Authorization", "Bearer " + accessToken);
    builder.addHeader("Content-Type", "application/x-protobuf");
    builder.addHeader("User-Agent", USER_AGENT);
    builder.addHeader("Accept-Encoding", "gzip");
    builder.setContentLength((int) payload.getLength());
    builder.setBody(payload.getMessage().toByteArray());
    return builder;
  }

  private InputStream streamResponse(final Response response) throws IOException {
    final InputStream input = response.getResponseBodyAsStream();
    final boolean compressed = "gzip".equals(response.getHeader("Content-Encoding"));
    return compressed ? new GZIPInputStream(input) : input;
  }

  @Override
  public TransactionResult transaction() throws DatastoreException {
    return Futures.get(transactionAsync(), DatastoreException.class);
  }

  @Override
  public ListenableFuture<TransactionResult> transactionAsync() {
    final ListenableFuture<Response> httpResponse;
    try {
      final BeginTransactionRequest.Builder request = BeginTransactionRequest.newBuilder();
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = ListenableFutureAdapter.asGuavaFuture(prepareRequest("beginTransaction", payload).execute());
    } catch (final Exception e) {
      return Futures.immediateFailedFuture(new DatastoreException(e));
    }
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final BeginTransactionResponse transaction = BeginTransactionResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(TransactionResult.build(transaction));
    });
  }

  @Override
  public RollbackResult rollback(final TransactionResult txn) throws DatastoreException {
    return Futures.get(rollbackAsync(Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<RollbackResult> rollbackAsync(final ListenableFuture<TransactionResult> txn) {
    final ListenableFuture<Response> httpResponse = Futures.transform(txn, (TransactionResult result) -> {
      final ByteString transaction = result.getTransaction();
      if (transaction == null) {
        throw new DatastoreException("Invalid transaction.");
      }
      final RollbackRequest.Builder request = RollbackRequest.newBuilder();
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      return ListenableFutureAdapter.asGuavaFuture(prepareRequest("rollback", payload).execute());
    });
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final RollbackResponse rollback = RollbackResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(RollbackResult.build(rollback));
    });
  }

  @Override
  public MutationResult commit(final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync((MutationStatement) null, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<MutationResult> commitAsync(final ListenableFuture<TransactionResult> txn) {
    return executeAsync((MutationStatement) null, txn);
  }

  @Override
  public AllocateIdsResult execute(final AllocateIds statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  @Override
  public ListenableFuture<AllocateIdsResult> executeAsync(final AllocateIds statement) {
    final ListenableFuture<Response> httpResponse;
    try {
      final AllocateIdsRequest.Builder request = AllocateIdsRequest.newBuilder()
          .addAllKeys(statement.getPb(config.getNamespace()));
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      httpResponse = ListenableFutureAdapter.asGuavaFuture(prepareRequest("allocateIds", payload).execute());
    } catch (final Exception e) {
      return Futures.immediateFailedFuture(new DatastoreException(e));
    }
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final AllocateIdsResponse allocate = AllocateIdsResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(AllocateIdsResult.build(allocate));
    });
  }

  @Override
  public QueryResult execute(final KeyQuery statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  @Override
  public QueryResult execute(final List<KeyQuery> statements) throws DatastoreException {
    return Futures.get(executeAsync(statements), DatastoreException.class);
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final KeyQuery statement) {
    return executeAsync(statement, Futures.immediateFuture(TransactionResult.build()));
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final List<KeyQuery> statements) {
    return executeAsync(statements, Futures.immediateFuture(TransactionResult.build()));
  }

  @Override
  public QueryResult execute(final KeyQuery statement, final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync(statement, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public QueryResult execute(final List<KeyQuery> statements, final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync(statements, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final KeyQuery statement, final ListenableFuture<TransactionResult> txn) {
    return executeAsync(ImmutableList.of(statement), txn);
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final List<KeyQuery> statements, final ListenableFuture<TransactionResult> txn) {
    final ListenableFuture<Response> httpResponse = Futures.transform(txn, (TransactionResult result) -> {
      final List<com.google.datastore.v1.Key> keys = statements
        .stream().map(s -> s.getKey().getPb(config.getNamespace())).collect(Collectors.toList());
      final LookupRequest.Builder request = LookupRequest.newBuilder().addAllKeys(keys);
      final ByteString transaction = result.getTransaction();
      if (transaction != null) {
        request.setReadOptions(ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      return ListenableFutureAdapter.asGuavaFuture(prepareRequest("lookup", payload).execute());
    });
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final LookupResponse query = LookupResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(QueryResult.build(query));
    });
  }

  @Override
  public MutationResult execute(final MutationStatement statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(final MutationStatement statement) {
    return executeAsync(statement, Futures.immediateFuture(TransactionResult.build()));
  }

  @Override
  public MutationResult execute(final MutationStatement statement, final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync(statement, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(final MutationStatement statement, final ListenableFuture<TransactionResult> txn) {
    final List<Mutation> mutations = Optional
      .ofNullable(statement)
      .flatMap(s -> Optional.of(ImmutableList.of(s.getPb(config.getNamespace()))))
      .orElse(ImmutableList.of());

    return executeAsyncMutations(mutations, txn);
  }

  @Override
  public MutationResult execute(final Batch batch) throws DatastoreException {
    return Futures.get(executeAsync(batch), DatastoreException.class);
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(final Batch batch) {
    return executeAsync(batch, Futures.immediateFuture(TransactionResult.build()));
  }

  @Override
  public MutationResult execute(final Batch batch, final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync(batch, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<MutationResult> executeAsync(final Batch batch, final ListenableFuture<TransactionResult> txn) {
    return executeAsyncMutations(batch.getPb(config.getNamespace()), txn);
  }

  private ListenableFuture<MutationResult> executeAsyncMutations(final List<Mutation> mutations, final ListenableFuture<TransactionResult> txn) {
    final ListenableFuture<Response> httpResponse = Futures.transform(txn, (TransactionResult result) -> {
      final CommitRequest.Builder request = CommitRequest.newBuilder();
      if (mutations != null) {
        request.addAllMutations(mutations);
      }

      final ByteString transaction = result.getTransaction();
      if (transaction != null) {
        request.setTransaction(transaction);
      } else {
        request.setMode(CommitRequest.Mode.NON_TRANSACTIONAL);
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      return ListenableFutureAdapter.asGuavaFuture(prepareRequest("commit", payload).execute());
    });
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final CommitResponse commit = CommitResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(MutationResult.build(commit));
    });
  }

  @Override
  public QueryResult execute(final Query statement) throws DatastoreException {
    return Futures.get(executeAsync(statement), DatastoreException.class);
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final Query statement) {
    return executeAsync(statement, Futures.immediateFuture(TransactionResult.build()));
  }

  @Override
  public QueryResult execute(final Query statement, final TransactionResult txn) throws DatastoreException {
    return Futures.get(executeAsync(statement, Futures.immediateFuture(txn)), DatastoreException.class);
  }

  @Override
  public ListenableFuture<QueryResult> executeAsync(final Query statement, final ListenableFuture<TransactionResult> txn) {
    final ListenableFuture<Response> httpResponse = Futures.transform(txn, (TransactionResult result) -> {
      final String namespace = config.getNamespace();
      final RunQueryRequest.Builder request = RunQueryRequest.newBuilder()
        .setQuery(statement.getPb(namespace != null ? namespace : ""));
      if (namespace != null) {
        request.setPartitionId(PartitionId.newBuilder().setNamespaceId(namespace));
      }
      final ByteString transaction = result.getTransaction();
      if (transaction != null) {
        request.setReadOptions(ReadOptions.newBuilder().setTransaction(transaction));
      }
      final ProtoHttpContent payload = new ProtoHttpContent(request.build());
      return ListenableFutureAdapter.asGuavaFuture(prepareRequest("runQuery", payload).execute());
    });
    return Futures.transform(httpResponse, (Response response) -> {
      if (!isSuccessful(response.getStatusCode())) {
        throw new DatastoreException(response.getStatusCode(), response.getResponseBody());
      }
      final RunQueryResponse query = RunQueryResponse.parseFrom(streamResponse(response));
      return Futures.immediateFuture(QueryResult.build(query));
    });
  }
}
