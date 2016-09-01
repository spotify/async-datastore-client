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
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Represents an entity key.
 *
 * A key is immutable; use {@code Key.builder()} to construct new
 * {@code Key} instances.
 */
public final class Key {

  /**
   * Represents an key path element.
   *
   * A key path element is immutable.
   */
  public static final class Element {

    private final com.google.datastore.v1.Key.PathElement element;

    private Element(final com.google.datastore.v1.Key.PathElement element) {
      this.element = element;
    }

    public String getKind() {
      return element.getKind();
    }

    public Long getId() {
      return isId(element) ? element.getId() : null;
    }

    public String getName() {
      return isName(element) ? element.getName() : null;
    }

    com.google.datastore.v1.Key.PathElement getPb() {
      return element;
    }

    @Override
    public String toString() {
      final Long id = getId();
      return getKind() + ":" + (id == null ? getName() : id);
    }
  }

  private final com.google.datastore.v1.Key key;

  private Key(final com.google.datastore.v1.Key key) {
    this.key = key;
  }

  public static final class Builder {

    private final com.google.datastore.v1.Key.Builder key;

    private Builder() {
      this.key = com.google.datastore.v1.Key.newBuilder();
    }

    private Builder(final Key key) {
      this(key.getPb());
    }

    private Builder(final com.google.datastore.v1.Key key) {
      this.key = com.google.datastore.v1.Key.newBuilder(key);
    }

    /**
     * Creates a new {@code Key}.
     *
     * @return an immutable key.
     */
    public Key build() {
      return new Key(key.build());
    }

    /**
     * Set the namespace for this {@code Key}.
     *
     * @param namespace the namespace to set.
     * @return this key builder.
     */
    public Builder namespace(final String namespace) {
      key.setPartitionId(
          com.google.datastore.v1.PartitionId.newBuilder().setNamespaceId(namespace));
      return this;
    }

    /**
     * Add a path element to this key.
     *
     * @param element the path element to add.
     * @return this key builder.
     */
    public Builder path(final Element element) {
      key.addPath(element.getPb());
      return this;
    }

    /**
     * Add a path element of a given kind to this key.
     *
     * @param kind the path element kind.
     * @return this key builder.
     */
    public Builder path(final String kind) {
      key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind));
      return this;
    }

    /**
     * Add a path element of a given kind and id to this key.
     *
     * @param kind the path element kind.
     * @param id the path element id.
     * @return this key builder.
     */
    public Builder path(final String kind, final long id) {
      key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind).setId(id));
      return this;
    }

    /**
     * Add a path element of a given kind and name to this key.
     *
     * @param kind the path element kind.
     * @param name the path element name.
     * @return this key builder.
     */
    public Builder path(final String kind, final String name) {
      key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind).setName(name));
      return this;
    }

    /**
     * Add a given key as a parent of this key.
     *
     * @param parent the parent key to add.
     * @return this key builder.
     */
    public Builder parent(final Key parent) {
      for (final Element element : parent.getPath()) {
        final Long id = element.getId();
        final String name = element.getName();
        if (id != null) {
          path(element.getKind(), id);
        } else if (name != null) {
          path(element.getKind(), name);
        }
      }
      return this;
    }
  }

  /**
   * Creates a new empty {@code Key} builder.
   *
   * @return an key builder.
   */
  public static Key.Builder builder() {
    return new Key.Builder();
  }

  /**
   * Creates a new {@code Key} builder for a given kind.
   *
   * This is a shortcut for {@code Key.builder().path(kind).build()}
   *
   * @param kind the kind of entity key.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind) {
    return new Key.Builder().path(kind);
  }

  /**
   * Creates a new {@code Key} builder for a given kind with a parent.
   *
   * This is a shortcut for {@code Key.builder().parent(parent).path(kind).build()}
   *
   * @param kind the kind of entity key.
   * @param parent the parent key to add.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind, final Key parent) {
    return new Key.Builder().parent(parent).path(kind);
  }

  /**
   * Creates a new {@code Key} builder for a given kind and an id.
   *
   * This is a shortcut for {@code Key.builder().path(kind, id).build()}
   *
   * @param kind the kind of entity key.
   * @param id the id of entity key.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind, final long id) {
    return new Key.Builder().path(kind, id);
  }

  /**
   * Creates a new {@code Key} builder for a given kind and an id with a parent.
   *
   * This is a shortcut for {@code Key.builder().parent(parent).path(kind, id).build()}
   *
   * @param kind the kind of entity key.
   * @param id the id of entity key.
   * @param parent the parent key to add.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind, final long id, final Key parent) {
    return new Key.Builder().parent(parent).path(kind, id);
  }

  /**
   * Creates a new {@code Key} builder for a given kind and a name.
   *
   * This is a shortcut for {@code Key.builder().path(kind, name).build()}
   *
   * @param kind the kind of entity key.
   * @param name the name of entity key.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind, final String name) {
    return new Key.Builder().path(kind, name);
  }

  /**
   * Creates a new {@code Key} builder for a given kind and an name with a parent.
   *
   * This is a shortcut for {@code Key.builder().parent(parent).path(kind, name).build()}
   *
   * @param kind the kind of entity key.
   * @param name the name of entity key.
   * @param parent the parent key to add.
   * @return a key builder.
   */
  public static Key.Builder builder(final String kind, final String name, final Key parent) {
    return new Key.Builder().parent(parent).path(kind, name);
  }

  /**
   * Creates a new {@code Key} builder based on an existing key.
   *
   * @param key the key to base this builder.
   * @return a key builder.
   */
  public static Key.Builder builder(final Key key) {
    return new Key.Builder(key);
  }

  static Key.Builder builder(final com.google.datastore.v1.Key key) {
    return new Key.Builder(key);
  }

  /**
   * Return the namespace for this key.
   *
   * @return the namespace.
   */
  public String getNamespace() {
    return key.getPartitionId().getNamespaceId();
  }

  /**
   * Return whether this key is complete or not.
   *
   * A complete key is a key that includes a "kind" and ("id" or "name").
   *
   * @return true if the key is complete.
   */
  public boolean isComplete() {
    if (key.getPathCount() == 0) {
      return false;
    }

    return key.getPathList().stream().allMatch(e -> isId(e) || isName(e));
  }

  /**
   * Return element kind of key, or null if not set.
   *
   * This is a shortcut for {@code Key.getPath().get(Key.getPath().size() - 1).getKind()}
   *
   * @return the element kind.
   */
  public String getKind() {
    final com.google.datastore.v1.Key.PathElement element = Iterables.getLast(key.getPathList(), null);
    if (element == null) {
      return null;
    }
    return element.getKind();
  }

  /**
   * Return element key id, or null if not set.
   *
   * This is a shortcut for {@code Key.getPath().get(Key.getPath().size() - 1).getId()}
   *
   * @return the key id.
   */
  public Long getId() {
    final com.google.datastore.v1.Key.PathElement element = Iterables.getLast(key.getPathList(), null);
    if (element == null) {
      return null;
    }
    return isId(element) ? element.getId() : null;
  }

  /**
   * Return element key name, or null if not set.
   *
   * This is a shortcut for {@code Key.getPath().get(Key.getPath().size() - 1).getName()}
   *
   * @return the key name.
   */
  public String getName() {
    final com.google.datastore.v1.Key.PathElement element = Iterables.getLast(key.getPathList(), null);
    if (element == null) {
      return null;
    }
    return isName(element) ? element.getName() : null;
  }

  /**
   * Return element path that represents this key.
   *
   * @return a list of path elements that make up this key.
   */
  public List<Element> getPath() {
    return ImmutableList.copyOf(key.getPathList().stream()
                                    .map(Element::new)
                                    .collect(Collectors.toList()));
  }

  @Override
  public String toString() {
    return "{" + getPath().stream()
        .map(Element::toString)
        .collect(Collectors.joining(", ")) + "}";
  }

  @Override
  public int hashCode() {
    return key.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    return obj == this || (obj instanceof Key && Objects.equals(key, ((Key) obj).key));
  }

  com.google.datastore.v1.Key getPb() {
    return key;
  }

  com.google.datastore.v1.Key getPb(final String namespace) {
    if (namespace == null) {
      return key;
    } else {
      return com.google.datastore.v1.Key
          .newBuilder(key)
          .setPartitionId(
              com.google.datastore.v1.PartitionId
                  .newBuilder().setNamespaceId(namespace)).build();
    }
  }

  private static boolean isId(final com.google.datastore.v1.Key.PathElement element) {
    return element.getIdTypeCase() == com.google.datastore.v1.Key.PathElement.IdTypeCase.ID;
  }

  private static boolean isName(final com.google.datastore.v1.Key.PathElement element) {
    return element.getIdTypeCase() == com.google.datastore.v1.Key.PathElement.IdTypeCase.NAME;
  }
}
