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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import org.junit.After;
import org.junit.Before;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public abstract class DatastoreTest {

  public static final String DATASTORE_HOST = System.getProperty("host", "http://localhost:8080");
  public static final String PROJECT = System.getProperty("dataset", "async-test");
  public static final String NAMESPACE = System.getProperty("namespace", "test");
  public static final String KEY_PATH = System.getProperty("keypath");
  public static final String VERSION = System.getProperty("version", "v1beta3");

  protected static Datastore datastore;

  @Before
  public void before() throws Exception {
    datastore = Datastore.create(datastoreConfig());
    resetDatastore();
  }

  private DatastoreConfig datastoreConfig() {
    final DatastoreConfig.Builder config = DatastoreConfig.builder()
        .connectTimeout(5000)
        .requestTimeout(1000)
        .maxConnections(5)
        .requestRetry(3)
        .version(VERSION)
        .host(DATASTORE_HOST)
        .project(PROJECT);

    if (NAMESPACE != null) {
      config.namespace(NAMESPACE);
    }

    if (KEY_PATH != null) {
      try {
        FileInputStream creds = new FileInputStream(new File(KEY_PATH));
        config.credential(GoogleCredential.fromStream(creds).createScoped(DatastoreConfig.SCOPES));
      }  catch (final IOException e) {
        System.err.println("Failed to load credentials " + e.getMessage());
        System.exit(1);
      }
    }

    return config.build();
  }

  private void resetDatastore() throws Exception {
    // add other kinds here as necessary...
    removeAll("employee");
    removeAll("payments");
  }

  private void removeAll(final String kind) throws Exception {
    final Query queryAll = QueryBuilder.query().kindOf(kind).keysOnly();
    for (final Entity entity : datastore.execute(queryAll)) {
      datastore.execute(QueryBuilder.delete(entity.getKey()));
    }
  }

  @After
  public void after() throws Exception {
    datastore.close();
  }
}
