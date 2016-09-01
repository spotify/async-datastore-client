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
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.firstNonNull;

/**
 * Datastore configuration class used to initialise {@code Datastore}.
 * <p>
 * Use {@code DatastoreConfig.builder()} build a config object by supplying
 * options such as {@code connectTimeout()} and {@code project()}.
 * <p>
 * Defaults are assigned for any options not provided.
 */
public final class DatastoreConfig {

  public static final List<String> SCOPES = ImmutableList.of(
    "https://www.googleapis.com/auth/datastore");

  private static final Integer DEFAULT_CONNECT_TIMEOUT = 5000;
  private static final Integer DEFAULT_MAX_CONNECTIONS = -1;
  private static final Integer DEFAULT_REQUEST_TIMEOUTS = 5000;
  private static final Integer DEFAULT_REQUEST_RETRIES = 5;
  private static final String DEFAULT_HOST = "https://datastore.googleapis.com";
  private static final String DEFAULT_VERSION = "v1";

  private final int connectTimeout;
  private final int maxConnections;
  private final int requestTimeout;
  private final int requestRetry;
  private final Credential credential;
  private final String project;
  private final String namespace;
  private final String host;
  private final String version;

  private DatastoreConfig(final Integer connectTimeout,
                          final Integer maxConnections,
                          final Integer requestTimeout,
                          final Integer requestRetry,
                          final Credential credential,
                          final String project,
                          final String namespace,
                          final String host,
                          final String version) {
    this.connectTimeout = firstNonNull(connectTimeout, DEFAULT_CONNECT_TIMEOUT);
    this.maxConnections = firstNonNull(maxConnections, DEFAULT_MAX_CONNECTIONS);
    this.requestTimeout = firstNonNull(requestTimeout, DEFAULT_REQUEST_TIMEOUTS);
    this.requestRetry = firstNonNull(requestRetry, DEFAULT_REQUEST_RETRIES);
    this.credential = credential;
    this.project = project;
    this.namespace = namespace;
    this.host = firstNonNull(host, DEFAULT_HOST);
    this.version = firstNonNull(version, DEFAULT_VERSION);
  }

  public static final class Builder {
    private Integer connectTimeout;
    private Integer maxConnections;
    private Integer requestTimeout;
    private Integer requestRetry;
    private Credential credential;
    private String project;
    private String namespace;
    private String host;
    private String version;

    private Builder() {}

    /**
     * Creates a new {@code DatastoreConfig}.
     *
     * @return an immutable config.
     */
    public DatastoreConfig build() {
      return new DatastoreConfig(connectTimeout,
                                 maxConnections,
                                 requestTimeout,
                                 requestRetry,
                                 credential,
                                 project,
                                 namespace,
                                 host,
                                 version);
    }

    /**
     * Set the maximum time in milliseconds the client will can wait
     * when connecting to a remote host.
     *
     * @param connectTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder connectTimeout(final int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Set the maximum number of connections client will handle.
     *
     * @param maxConnections the maximum number of connections.
     * @return this config builder.
     */
    public Builder maxConnections(final int maxConnections) {
      this.maxConnections = maxConnections;
      return this;
    }

    /**
     * Set the maximum time in milliseconds the client waits until
     * the response is completed.
     *
     * @param requestTimeout the maximum time in milliseconds.
     * @return this config builder.
     */
    public Builder requestTimeout(final int requestTimeout) {
      this.requestTimeout = requestTimeout;
      return this;
    }

    /**
     * Set the number of times a request will be retried when an
     * {@link java.io.IOException} occurs because of a Network exception.
     *
     * @param requestRetry the number of times a request will be retried.
     * @return this config builder.
     */
    public Builder requestRetry(final int requestRetry) {
      this.requestRetry = requestRetry;
      return this;
    }

    /**
     * Set Datastore credentials to ues when requesting an access token.
     * <p>
     * Credentials can be generated by calling
     * {@code DatastoreHelper.getComputeEngineCredential} or
     * {@code DatastoreHelper.getServiceAccountCredential}
     *
     * @param credential the credentials used to authenticate.
     * @return this config builder.
     */
    public Builder credential(final Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * Set project id to use when querying Datastore.
     *
     * @param project the project id.
     * @return this config builder.
     */
    public Builder project(final String project) {
      this.project = project;
      return this;
    }

    /**
     * An optional namespace may be specified to further partition data in
     * your project.
     *
     * @param namespace the namespace.
     * @return this config builder.
     */
    public Builder namespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }

    /**
     * The Datastore host to connect to. By default, this is the Google
     * Datastore provider, however you may run a local Developer Server.
     *
     * @param host the host to connect to.
     * @return this config builder.
     */
    public Builder host(final String host) {
      this.host = host;
      return this;
    }

    /**
     * The Datastore API version to use.
     *
     * @param version the version to use.
     * @return this config builder.
     */
    public Builder version(final String version) {
      this.version = version;
      return this;
    }
  }

  public static DatastoreConfig.Builder builder() {
    return new DatastoreConfig.Builder();
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getMaxConnections() {
    return maxConnections;
  }

  public int getRequestTimeout() {
    return requestTimeout;
  }

  public int getRequestRetry() {
    return requestRetry;
  }

  public Credential getCredential() {
    return credential;
  }

  public String getProject() {
    return project;
  }

  public String getNamespace() {
    return namespace;
  }

  public String getHost() {
    return host;
  }

  public String getVersion() {
    return version;
  }
}

