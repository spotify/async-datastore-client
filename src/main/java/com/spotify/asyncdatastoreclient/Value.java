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

import com.google.common.collect.ImmutableList;
import com.google.datastore.v1.ArrayValue;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;

import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents an entity property value.
 *
 * A value is immutable; use {@code Value.builder()} to construct new
 * {@code Value} instances.
 */
public final class Value {

  private final com.google.datastore.v1.Value value;

  private Value(final com.google.datastore.v1.Value value) {
    this.value = value;
  }

  public static final class Builder {

    private final com.google.datastore.v1.Value.Builder value;

    private boolean excludeFromIndexes;

    private Builder(com.google.datastore.v1.Value.Builder builder) {
        this.value = builder;
    }

    private Builder() {
      this.value = com.google.datastore.v1.Value.newBuilder();
      this.excludeFromIndexes = false;
    }

    private Builder(final Value value) {
      this(value.getPb());
    }

    private Builder(final com.google.datastore.v1.Value value) {
      this.value = com.google.datastore.v1.Value.newBuilder(value);
      this.excludeFromIndexes = value.getExcludeFromIndexes();
    }

    /**
     * Creates a new {@code Value}.
     *
     * @return an immutable value.
     */
    public Value build() {
      return new Value(value.setExcludeFromIndexes(excludeFromIndexes).build());
    }

    /**
     * Set the value for this {@code Value}.
     * <p>
     * The supplied value must comply with the data types supported by Datastore.
     *
     * @param value the value to set.
     * @return this value builder.
     * @throws IllegalArgumentException if supplied {@code value} is not recognised.
     * @deprecated Use type-specific builders instead, like {@code Value#fromString(String)}.
     */
    public Builder value(final Object value) {
      if (value instanceof String) {
        this.value.setStringValue((String) value);
      } else if (value instanceof Boolean) {
        this.value.setBooleanValue((Boolean) value);
      } else if (value instanceof Date) {
        this.value.setTimestampValue(toTimestamp((Date) value));
      } else if (value instanceof ByteString) {
        this.value.setBlobValue((ByteString) value);
      } else if (value instanceof Entity) {
        this.value.setEntityValue(((Entity) value).getPb()).setExcludeFromIndexes(true);
      } else if (value instanceof Key) {
        this.value.setKeyValue(((Key) value).getPb());
      } else if (value instanceof Double) {
        this.value.setDoubleValue((Double) value);
      } else if (value instanceof Long) {
        this.value.setIntegerValue((Long) value);
      } else if (value instanceof Float) {
        this.value.setDoubleValue(((Float) value).doubleValue());
      } else if (value instanceof Integer) {
        this.value.setIntegerValue(((Integer) value).longValue());
      } else {
        throw new IllegalArgumentException("Invalid value type: " + value.getClass());
      }
      return this;
    }

    /**
     * Set a list of values for this {@code Value}.
     * <p>
     * The supplied value items must comply with the data types supported by Datastore.
     *
     * @param values a list of values to set.
     * @return this value builder.
     * @throws IllegalArgumentException if supplied {@code values} contains types
     * that are not recognised.
     */
    public Builder value(final List<Object> values) {
      final ArrayValue arrayValues = ArrayValue
          .newBuilder()
          .addAllValues(
              values
                  .stream()
                  .map(v -> Value.builder(v).build().getPb()).collect(Collectors.toList()))
          .build();

      this.value.setArrayValue(arrayValues);

      return this;
    }

    /**
     * Set a whether this value should be indexed or not.
     *
     * @param indexed indicates whether value is indexed.
     * @return this value builder.
     */
    public Builder indexed(final boolean indexed) {
      this.excludeFromIndexes = !indexed;
      return this;
    }

  }

  private static Timestamp toTimestamp(Date date) {
    final long millis = date.getTime();
    return Timestamp
        .newBuilder()
        .setSeconds(millis / 1000)
        .setNanos((int) ((millis % 1000) * 1000000))
        .build();
  }

  /**
   * Creates a new empty {@code Value} builder.
   *
   * @return an value builder.
   */
  public static Value.Builder builder() {
    return new Value.Builder();
  }

  /**
   * Create a new value containing a string.
   *
   * @param value The string to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final String value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setStringValue(value));
  }

  /**
   * Create a new value containing a boolean.
   *
   * @param value The boolean to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final boolean value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setBooleanValue(value));
  }

  /**
   * Create a new value containing a date.
   *
   * @param value The date to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final Date value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setTimestampValue(toTimestamp(value)));
  }

  /**
   * Create a new value containing a blob.
   *
   * @param value The blob to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final ByteString value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setBlobValue(value));
  }

  /**
   * Create a new value containing a entity.
   *
   * @param entity The entity to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final Entity entity) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setEntityValue(entity.getPb()));
  }

  /**
   * Create a new value containing a key.
   *
   * @param key The key to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final Key key) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setKeyValue(key.getPb()));
  }

  /**
   * Create a new value containing a double.
   *
   * @param value The double to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final double value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setDoubleValue(value));
  }

  /**
   * Create a new value containing a float.
   *
   * @param value The float to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final float value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setDoubleValue(value));
  }

  /**
   * Create a new value containing a integer.
   *
   * @param value The integer to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final int value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setIntegerValue(value));
  }

  /**
   * Create a new value containing a long.
   *
   * @param value The long to build a value from.
   * @return A new value builder.
   */
  public static Value.Builder from(final long value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setIntegerValue(value));
  }

  /**
   * Create a new value containing an existing value.
   *
   * @param value Values to add to builder.
   * @return A new value builder containing a list.
   */
  public static Value.Builder from(final Value value) {
    return new Value.Builder(com.google.datastore.v1.Value.newBuilder(value.getPb()));
  }

  /**
   * Create a new value containing a list of values.
   *
   * @param values List of values to add to builder.
   * @return A new value builder containing a list.
   */
  public static Value.Builder from(final List<Value> values) {
    final com.google.datastore.v1.ArrayValue.Builder builder = com.google.datastore.v1.ArrayValue.newBuilder();

    values.stream().forEach(v -> builder.addValues(v.getPb()));

    return new Value.Builder(com.google.datastore.v1.Value.newBuilder().setArrayValue(builder).build());
  }

  /**
   * Create a new value containing an array of values.
   *
   * @param first First value to add.
   * @param values Array of values to add to builder.
   * @return A new value builder containing a list.
   */
  public static Value.Builder from(final Value first, final Value... values) {
    final com.google.datastore.v1.ArrayValue.Builder builder =
      com.google.datastore.v1.ArrayValue.newBuilder();

    builder.addValues(first.getPb());
    for (final Value v : values) {
      builder.addValues(v.getPb());
    }

    return new Value.Builder(
      com.google.datastore.v1.Value.newBuilder().setArrayValue(builder).build());
  }

  /**
   * Creates a new {@code Value} builder based on an existing value.
   *
   * @param value the value to use as a base.
   * @return an value builder.
   * @deprecated Use {@link #from(Value)}.
   */
  public static Value.Builder builder(final Value value) {
    return new Value.Builder(value);
  }

  /**
   * Creates a new {@code Value} builder based on a given value.
   *
   * @param value the value to set.
   * @return an value builder.
   * @deprecated Prefer value-specific builders, like {@link #from(String)}.
   */
  public static Value.Builder builder(final Object value) {
    return new Builder().value(value);
  }

  /**
   * Creates a new {@code Value} builder based on a given list of values.
   *
   * @param values the list of values to set.
   * @return an value builder.
   */
  public static Value.Builder builder(final List<Object> values) {
    return new Builder().value(values);
  }

  static Value.Builder builder(final com.google.datastore.v1.Value value) {
    return new Value.Builder(value);
  }

  /**
   * Return the value as a string.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a string.
   */
  public String getString() {
    if (!isString()) {
      throw new IllegalArgumentException("Value does not contain a string.");
    }
    return value.getStringValue();
  }

  public boolean isString() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.STRING_VALUE;
  }

  /**
   * Return the value as an integer.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not an integer.
   */
  public long getInteger() {
    if (!isInteger()) {
      throw new IllegalArgumentException("Value does not contain an integer.");
    }
    return value.getIntegerValue();
  }

  /**
   * Check if value is a integer.
   *
   * @return {@code true} if value is a integer.
   */
  public boolean isInteger() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.INTEGER_VALUE;
  }

  /**
   * Return the value as a boolean.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a boolean.
   */
  public boolean getBoolean() {
    if (!isBoolean()) {
      throw new IllegalArgumentException("Value does not contain a boolean.");
    }
    return value.getBooleanValue();
  }

  /**
   * Check if value is a boolean.
   *
   * @return {@code true} if value is a boolean.
   */
  public boolean isBoolean() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.BOOLEAN_VALUE;
  }

  /**
   * Return the value as a double.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a double.
   */
  public double getDouble() {
    if (!isDouble()) {
      throw new IllegalArgumentException("Value does not contain a double.");
    }
    return value.getDoubleValue();
  }

  /**
   * Check if value is a double.
   *
   * @return {@code true} if value is a double.
   */
  public boolean isDouble() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.DOUBLE_VALUE;
  }

  /**
   * Return the value as a date.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a date.
   */
  public Date getDate() {
    if (!isDate()) {
      throw new IllegalArgumentException("Value does not contain a timestamp.");
    }

    return toDate(value.getTimestampValue());
  }

  /**
   * Check if value is a date.
   *
   * @return {@code true} if value is a date.
   */
  public boolean isDate() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.TIMESTAMP_VALUE;
  }

  /**
   * Return the value as a blob.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a blob.
   */
  public ByteString getBlob() {
    if (!isBlob()) {
      throw new IllegalArgumentException("Value does not contain a blob.");
    }
    return value.getBlobValue();
  }

  /**
   * Check if value is a blob.
   *
   * @return {@code true} if value is a blob.
   */
  public boolean isBlob() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.BLOB_VALUE;
  }

  /**
   * Return the value as an {@code Entity}.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not an entity.
   */
  public Entity getEntity() {
    if (!isEntity()) {
      throw new IllegalArgumentException("Value does not contain an entity.");
    }
    return Entity.builder(value.getEntityValue()).build();
  }

  /**
   * Check if value is a entity.
   *
   * @return {@code true} if value is a entity.
   */
  public boolean isEntity() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.ENTITY_VALUE;
  }

  /**
   * Return the value as a {@code Key}.
   *
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a key.
   */
  public Key getKey() {
    final com.google.datastore.v1.Key key = value.getKeyValue();
    if (key == null) {
      throw new IllegalArgumentException("Value does not contain an key.");
    }
    return Key.builder(key).build();
  }

  /**
   * Check if value is a key.
   *
   * @return {@code true} if value is a key.
   */
  public boolean isKey() {
    return value.getKeyValue() != null;
  }

  /**
   * Return the value as a list of {@code Value}.
   *
   * @return the list value, an empty list indicates that the value is not set.
   */
  public List<Value> getList() {
    return ImmutableList.copyOf(
      value.getArrayValue().getValuesList().stream().map(Value::new).iterator());
  }

  /**
   * Check if value is a list.
   * Empty lists are not detected as lists.
   *
   * @return {@code true} if value is a list.
   */
  public boolean isList() {
    return value.getValueTypeCase() == com.google.datastore.v1.Value.ValueTypeCase.ARRAY_VALUE;
  }

  /**
   * Return the value as a list of objects cast to a given type.
   *
   * @param clazz the type of class to cast values to.
   * @return the value.
   * @throws IllegalArgumentException if {@code Value} is not a list.
   */
  public <T> List<T> getList(final Class<T> clazz) {
    return ImmutableList.copyOf(getList().stream()
                                    .map(valueLocal -> valueLocal.convert(clazz))
                                    .collect(Collectors.toList()));
  }

  @SuppressWarnings({"unchecked"})
  <T> T convert(Class<T> clazz) {
    if (clazz.equals(String.class)) {
      return (T) getString();
    } else if (clazz.equals(Long.class)) {
      return (T) Long.valueOf(getInteger());
    } else if (clazz.equals(Double.class)) {
      return (T) Double.valueOf(getDouble());
    } else if (clazz.equals(Boolean.class)) {
      return (T) Boolean.valueOf(getBoolean());
    } else if (clazz.equals(Date.class)) {
      return (T) getDate();
    } else if (clazz.equals(ByteString.class)) {
      return (T) getBlob();
    } else if (clazz.equals(Entity.class)) {
      return (T) getEntity();
    } else if (clazz.equals(Key.class)) {
      return (T) getKey();
    } else {
      throw new IllegalArgumentException("Unrecognised value type.");
    }
  }

  /**
   * Returns whether the value is indexed or not.
   *
   * @return true if the value is indexed.
   */
  public boolean isIndexed() {
    return !value.getExcludeFromIndexes();
  }

  com.google.datastore.v1.Value getPb() {
    return value;
  }

  com.google.datastore.v1.Value getPb(final String namespace) {
    final com.google.datastore.v1.Key key = value.getKeyValue();
    if (key.getPathCount() > 0) {
      return com.google.datastore.v1.Value.
          newBuilder(value)
          .setKeyValue(getKey().getPb(namespace)).build();
    }
    return value;
  }

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return obj.getClass() == this.getClass() && (obj instanceof Value && Objects.equals(value, ((Value) obj).value));
  }

  @Override
  public String toString() {
    switch (value.getValueTypeCase()) {
      case KEY_VALUE:
        return getKey().toString();
      case STRING_VALUE:
        return value.getStringValue();
      case BLOB_VALUE:
        return "<binary>";
      case TIMESTAMP_VALUE:
        return getDate().toString();
      case INTEGER_VALUE:
        return String.valueOf(value.getIntegerValue());
      case DOUBLE_VALUE:
        return String.valueOf(value.getDoubleValue());
      case BOOLEAN_VALUE:
        return String.valueOf(value.getBooleanValue());
      case ENTITY_VALUE:
        return getEntity().toString();
      case ARRAY_VALUE:
        return "[" +
            getList()
                .stream()
                .map(Value::toString)
                .collect(Collectors.joining(", ")) + "]";
      case NULL_VALUE:
        return "<null>";
      default:
        return value.toString();
    }
  }

  private Date toDate(final Timestamp timestamp) {
    return Date.from(Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos()));
  }
}
