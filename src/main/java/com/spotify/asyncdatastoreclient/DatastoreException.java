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

/**
 * Datastore exception.
 * <p>
 * All Datastore and client exceptions are encapsulated or wrapped
 * by {@code DatastoreException}.
 */
public class DatastoreException extends Exception {

  private Integer statusCode = null;

  public DatastoreException(final String message) {
    super(message);
  }

  public DatastoreException(final int statusCode, final String message) {
    super(message);
    this.statusCode = statusCode;
  }

  public DatastoreException(final Throwable t) {
    super(t);
  }

  @Override
  public Throwable initCause(final Throwable cause) {
    if (cause instanceof DatastoreException) {
      statusCode = ((DatastoreException) cause).statusCode;
    }
    return this;
  }

  public Integer getStatusCode() {
    return statusCode;
  }
}
