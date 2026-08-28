/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.spi.connector;

import com.facebook.presto.common.type.Type;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Designates one version of a table. A version may be written by the user, as
 * {@code FOR VERSION AS OF} or {@code FOR TIMESTAMP AS OF}, or produced by the connector to say
 * which version a read would see. Both are the same idea, in the way that {@code HEAD~3} and a
 * commit hash are both revisions.
 *
 * <p>Three forms exist, distinguished by {@link VersionType}:
 * <ul>
 *   <li>{@code TIMESTAMP} and {@code VERSION} carry a value the user wrote, with an operator
 *       saying whether it means {@code AS OF} or {@code BEFORE}. The connector interprets the
 *       value; the engine only passes it through.
 *   <li>{@code LATEST} names whatever the table is at now, and carries no value.
 *   <li>{@code RESOLVED} is a concrete version the connector produced, carrying a payload that is
 *       opaque to the engine. Only the connector that produced it may interpret it.
 * </ul>
 *
 * <p>A {@code RESOLVED} version is compared by value, so a caller can hold on to one and later
 * detect that a table has moved on by obtaining it again and comparing. What counts as "moved on"
 * is the connector's decision: it must include anything that would change the rows or the schema a
 * read of that table produces, which is not always the same as the data changing. In Iceberg, for
 * instance, a schema change writes new table metadata without creating a snapshot.
 *
 * <p>A {@code RESOLVED} version is not required to be a version the user could have written, so it
 * cannot be assumed to round-trip through {@link ConnectorMetadata#getTableHandle}.
 *
 * @see ConnectorMetadata#getTableVersion
 */
public class ConnectorTableVersion
{
    public enum VersionType
    {
        TIMESTAMP,
        VERSION,
        LATEST,
        RESOLVED
    }

    public enum VersionOperator
    {
        EQUAL,
        LESS_THAN
    }

    private static final ConnectorTableVersion LATEST_VERSION = new ConnectorTableVersion(VersionType.LATEST, null, null, null);

    private final VersionType versionType;
    private final VersionOperator versionOperator;
    private final Type versionExpressionType;
    private final Object tableVersion;

    /** A version the user wrote, as {@code TIMESTAMP} or {@code VERSION}. */
    public ConnectorTableVersion(VersionType versionType, VersionOperator versionOperator, Type versionExpressionType, Object tableVersion)
    {
        this.versionType = requireNonNull(versionType, "versionType is null");
        if (versionType == VersionType.TIMESTAMP || versionType == VersionType.VERSION) {
            requireNonNull(versionOperator, "versionOperator is null");
            requireNonNull(versionExpressionType, "versionExpressionType is null");
            requireNonNull(tableVersion, "tableVersion is null");
        }
        this.versionOperator = versionOperator;
        this.versionExpressionType = versionExpressionType;
        this.tableVersion = tableVersion;
    }

    /** Names whatever version the table is at now. */
    public static ConnectorTableVersion latest()
    {
        return LATEST_VERSION;
    }

    /**
     * A concrete version produced by a connector. {@code state} is opaque to the engine and is
     * compared by value; see the class documentation for what it must cover.
     */
    public static ConnectorTableVersion resolved(Object state)
    {
        return new ConnectorTableVersion(VersionType.RESOLVED, null, null, requireNonNull(state, "state is null"));
    }

    public VersionType getVersionType()
    {
        return versionType;
    }

    /** Present only for the {@code TIMESTAMP} and {@code VERSION} forms. */
    public Optional<VersionOperator> getVersionOperatorIfPresent()
    {
        return Optional.ofNullable(versionOperator);
    }

    /**
     * @throws IllegalStateException if this version carries no operator
     */
    public VersionOperator getVersionOperator()
    {
        if (versionOperator == null) {
            throw new IllegalStateException("Version of type " + versionType + " carries no operator");
        }
        return versionOperator;
    }

    /**
     * @throws IllegalStateException if this version carries no value
     */
    public Type getVersionExpressionType()
    {
        if (versionExpressionType == null) {
            throw new IllegalStateException("Version of type " + versionType + " carries no expression type");
        }
        return versionExpressionType;
    }

    /**
     * The value the user wrote, or the opaque state of a {@code RESOLVED} version.
     *
     * @throws IllegalStateException if this version carries no value
     */
    public Object getTableVersion()
    {
        if (tableVersion == null) {
            throw new IllegalStateException("Version of type " + versionType + " carries no value");
        }
        return tableVersion;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        ConnectorTableVersion that = (ConnectorTableVersion) other;
        return versionType == that.versionType &&
                versionOperator == that.versionOperator &&
                Objects.equals(versionExpressionType, that.versionExpressionType) &&
                Objects.equals(tableVersion, that.tableVersion);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(versionType, versionOperator, versionExpressionType, tableVersion);
    }

    @Override
    public String toString()
    {
        return new StringBuilder("ConnectorTableVersion{")
                .append("tableVersionType=").append(versionType)
                .append(", versionExpressionType=").append(versionExpressionType)
                .append(", tableVersion=").append(tableVersion)
                .append('}')
                .toString();
    }
}
