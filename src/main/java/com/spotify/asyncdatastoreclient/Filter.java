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

/**
 * Represents query filter.
 *
 * A filter is composed of a property name, an operator and a value. Multiple
 * filters may be applied to a single {@code Query}.
 */
public class Filter {

  public enum Operator {
    LESS_THAN,
    LESS_THAN_OR_EQUAL,
    GREATER_THAN,
    GREATER_THAN_OR_EQUAL,
    EQUAL,
    HAS_ANCESTOR
  }

  private final String name;
  private final Operator op;
  private final Value value;

  Filter(final String name, final Operator op, final Value value) {
    this.name = name;
    this.op = op;
    this.value = value;
  }

  DatastoreV1.Filter getPb() {
    final DatastoreV1.PropertyFilter.Builder filter = DatastoreV1.PropertyFilter.newBuilder()
        .setProperty(DatastoreV1.PropertyReference.newBuilder().setName(name))
        .setValue(value.getPb());
    switch (op) {
      case LESS_THAN:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.LESS_THAN);
        break;
      case LESS_THAN_OR_EQUAL:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.LESS_THAN_OR_EQUAL);
        break;
      case GREATER_THAN:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.GREATER_THAN);
        break;
      case GREATER_THAN_OR_EQUAL:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.GREATER_THAN_OR_EQUAL);
        break;
      case EQUAL:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.EQUAL);
        break;
      case HAS_ANCESTOR:
        filter.setOperator(DatastoreV1.PropertyFilter.Operator.HAS_ANCESTOR);
        break;
    }
    return DatastoreV1.Filter.newBuilder().setPropertyFilter(filter).build();
  }
}
