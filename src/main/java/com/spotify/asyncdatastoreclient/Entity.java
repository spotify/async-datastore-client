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

import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Represents an entity that is stored in Datastore.
 *
 * All properties are immutable; use {@code Entity.builder()} to construct new
 * {@code Entity} instances.
 */
public final class Entity {

  private final com.google.datastore.v1.Entity entity;
  private Map<String, Value> properties;

  private Entity(final com.google.datastore.v1.Entity entity) {
    this.entity = entity;
    this.properties = null;
  }

  public static final class Builder {

    private com.google.datastore.v1.Entity.Builder entity;
    private Map<String, Value> properties;

    private Builder() {
      this.entity = com.google.datastore.v1.Entity.newBuilder();
      this.properties = Maps.newHashMap();
    }

    private Builder(final Key key) {
      this.entity = com.google.datastore.v1.Entity.newBuilder().setKey(key.getPb());
      this.properties = Maps.newHashMap();
    }

    private Builder(final Entity entity) {
      this(entity.getPb());
    }

    private Builder(final com.google.datastore.v1.Entity entity) {
      this.entity = com.google.datastore.v1.Entity.newBuilder(entity);
      this.properties = entity.getProperties().entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Value.builder(e.getValue()).build()));
    }

    /**
     * Creates a new {@code Entity}.
     *
     * @return an immutable entity.
     */
    public Entity build() {
      entity.getMutableProperties().clear();
      entity.putAllProperties(
          properties
              .entrySet()
              .stream()
              .collect(Collectors.toMap(
                  Map.Entry::getKey, e -> e.getValue().getPb().toBuilder().build())));

      return new Entity(entity.build());
    }

    /**
     * Set the key for this entity.
     *
     * @param key the key to set for this entity.
     * @return this entity builder.
     */
    public Builder key(final Key key) {
      entity.setKey(key.getPb());
      return this;
    }

    /**
     * Set property and its value for this entity.
     *
     * @param name the property name to set.
     * @param value the property value.
     * @return this entity builder.
     */
    public Builder property(final String name, final Value value) {
      properties.put(name, value);
      return this;
    }

    /**
     * Set property and its value for this entity.
     *
     * @param name the property name to set.
     * @param value the property value.
     * @return this entity builder.
     */
    public Builder property(final String name, final Object value) {
      properties.put(name, Value.builder(value).build());
      return this;
    }

    /**
     * Set property and its value for this entity.
     *
     * @param name the property name to set.
     * @param value the property value.
     * @param indexed indicates whether the value should be indexed or not.
     * @return this entity builder.
     */
    public Builder property(final String name, final Object value, final boolean indexed) {
      properties.put(name, Value.builder(value).indexed(indexed).build());
      return this;
    }

    /**
     * Set property and a list of value for this entity.
     *
     * @param name the property name to set.
     * @param values a list of value.
     * @return this entity builder.
     */
    public Builder property(final String name, final List<Object> values) {
      properties.put(name, Value.builder(values).build());
      return this;
    }

    /**
     * Remove a property from this entity.
     *
     * @param name the property name to remove.
     * @return this entity builder.
     */
    public Builder remove(final String name) {
      properties.remove(name);
      return this;
    }
  }

  /**
   * Creates a new empty {@code Entity} builder.
   *
   * @return an entity builder.
   */
  public static Entity.Builder builder() {
    return new Entity.Builder();
  }

  /**
   * Creates a new {@code Entity} builder for a given kind.
   *
   * This is a shortcut for {@code Entity.builder().key(Key.builder(kind).build())}
   *
   * @param kind the kind of entity.
   * @return an entity builder.
   */
  public static Entity.Builder builder(final String kind) {
    return new Entity.Builder(Key.builder(kind).build());
  }

  /**
   * Creates a new {@code Entity} builder for a given kind and key id.
   *
   * This is a shortcut for {@code Entity.builder().key(Key.builder(kind, id).build())}
   *
   * @param kind the kind of entity.
   * @param id the key id.
   * @return an entity builder.
   */
  public static Entity.Builder builder(final String kind, final long id) {
    return new Entity.Builder(Key.builder(kind, id).build());
  }

  /**
   * Creates a new {@code Entity} builder for a given kind and key name.
   *
   * This is a shortcut for {@code Entity.builder().key(Key.builder(kind, name).build())}
   *
   * @param kind the kind of entity.
   * @param name the key name.
   * @return an entity builder.
   */
  public static Entity.Builder builder(final String kind, final String name) {
    return new Entity.Builder(Key.builder(kind, name).build());
  }

  /**
   * Creates a new {@code Entity} builder for a given key.
   *
   * This is a shortcut for {@code Entity.builder().key(key).build())}
   *
   * @param key the key for this entity.
   * @return an entity builder.
   */
  public static Entity.Builder builder(final Key key) {
    return new Entity.Builder(key);
  }

  /**
   * Creates a new {@code Entity} builder based on an existing entity.
   *
   * @param entity the entity to use as a base.
   * @return an entity builder.
   */
  public static Entity.Builder builder(final Entity entity) {
    return new Entity.Builder(entity);
  }

  static Entity.Builder builder(final com.google.datastore.v1.Entity entity) {
    return new Entity.Builder(entity);
  }

  /**
   * Return the key for this entity.
   *
   * @return a {@code Key}.
   */
  public Key getKey() {
    return Key.builder(entity.getKey()).build();
  }

  /**
   * Return the value for a given property as a string, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public String getString(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getString();
  }

  /**
   * Return the value for a given property as an integer, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Long getInteger(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getInteger();
  }

  /**
   * Return the value for a given property as a boolean, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Boolean getBoolean(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getBoolean();
  }

  /**
   * Return the value for a given property as a double, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Double getDouble(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getDouble();
  }

  /**
   * Return the value for a given property as a date, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Date getDate(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getDate();
  }

  /**
   * Return the value for a given property as a blob, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public ByteString getBlob(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getBlob();
  }

  /**
   * Return the value for a given property as an entity, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Entity getEntity(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getEntity();
  }

  /**
   * Return the value for a given property as a key, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a property value.
   */
  public Key getKey(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getKey();
  }

  /**
   * Return the value for a given property as a list of {@code Key}, or null
   * if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @return a list of property values.
   */
  public List<Value> getList(final String name) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getList();
  }

  /**
   * Return the value for a given property as a list of values that are cast
   * to a given type, or null if the property doesn't exist.
   *
   * @param name the name of the property to get.
   * @param clazz the type of class to cast values to.
   * @return a list of property values.
   */
  public <T> List<T> getList(final String name, final Class<T> clazz) {
    final Value value = getProperties().get(name);
    return value == null ? null : value.getList(clazz);
  }

  /**
   * Return a map of properties to their values for this entity.
   *
   * @return a map of property values.
   */
  public Map<String, Value> getProperties() {
    if (properties == null) {
      properties = entity
          .getProperties()
          .entrySet()
          .stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> Value.builder(e.getValue()).build()));
    }
    return properties;
  }

  /**
   * Get the value for given property, or empty if none exists.
   *
   * @param name name of the property to get.
   * @return an optional containing a property value, or empty.
   */
  public Optional<Value> get(final String name) {
      return Optional.ofNullable(getProperties().get(name));
  }

  /**
   * Return whether a given property name exists in this entity.
   *
   * @param name the name of the property.
   * @return true if the property exists.
   */
  public boolean contains(final String name) {
    return getProperties().containsKey(name);
  }

  @Override
  public String toString() {
    return "{" + getProperties().entrySet().stream()
        .map(property -> property.getKey() + ":" + property.getValue())
        .collect(Collectors.joining(", ")) + "}";
  }

  @Override
  public int hashCode() {
    return entity.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return obj.getClass() == this.getClass() || (obj instanceof Entity && Objects.equals(entity, ((Entity) obj).entity));
  }

  com.google.datastore.v1.Entity getPb() {
    return entity;
  }

  com.google.datastore.v1.Entity getPb(final String namespace) {
    final Map<String, com.google.datastore.v1.Value> propertiesLocal = entity.getProperties()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(
            Map.Entry::getKey, e -> Value.builder(e.getValue()).build().getPb(namespace)));

    return com.google.datastore.v1.Entity
        .newBuilder()
        .putAllProperties(propertiesLocal)
        .setKey(getKey().getPb(namespace))
        .build();
  }
}
