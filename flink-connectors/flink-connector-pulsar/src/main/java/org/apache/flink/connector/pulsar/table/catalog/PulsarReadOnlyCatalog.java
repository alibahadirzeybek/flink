/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.table.catalog;

import org.apache.flink.connector.pulsar.table.catalog.impl.IncompatibleSchemaException;
import org.apache.flink.connector.pulsar.table.catalog.impl.PulsarReadOnlyCatalogSupport;
import org.apache.flink.connector.pulsar.table.catalog.impl.SchemaTranslator;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.factories.Factory;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Catalog implementation to use Pulsar to store metadatas for Flink tables/databases.
 *
 * <p>A {@link PulsarCatalog} offers two modes when mapping a Pulsar topic to a Flink table.
 *
 * <p>explicit table: an explict table refers to a table created using CREATE statements. In this
 * mode, users are allowed to create a table that is bind to an existing Pulsar topic. Users can
 * specify watermarks, metatdata fields utilize the verbose configuration options to customize the
 * table connector.
 *
 * <p>native table: an native table refers to a table created by the Catalog and not by users using
 * a 1-to-1 mapping from Flink table to Pulsar topic. Each existing Pulsar topic will be mapped to a
 * table under a database using the topic's tenant and namespace named like 'tenant/namespace'. The
 * mapped table has the same name as the Pulsar topic. This mode allows users to easily query from
 * existing Pulsar topics without explicitly create the table. It automatically determines the Flink
 * format to use based on the stored Pulsar schema in the Pulsar topic. This mode has some
 * limitations, such as users can't designate an watermark and thus can't use window aggregation
 * functions.
 *
 * <p>Each topic(except Pulsar system topics) is mapped to a native table, and users can create
 * arbitrary number of explicit tables that binds to one Pulsar topic besides the native table.
 */
public class PulsarReadOnlyCatalog extends AbstractCatalog {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCatalog.class);

    private final PulsarCatalogConfiguration catalogConfiguration;

    private PulsarReadOnlyCatalogSupport catalogSupport;

    public PulsarReadOnlyCatalog(
            String catalogName, PulsarCatalogConfiguration catalogConfiguration) {
        super(catalogName, "default/default");
        this.catalogConfiguration = catalogConfiguration;
    }

    @Override
    public Optional<Factory> getFactory() {
        return Optional.empty();
    }

    @Override
    public void open() throws CatalogException {
        if (catalogSupport == null) {
            try {
                catalogSupport =
                        new PulsarReadOnlyCatalogSupport(
                                catalogConfiguration, new SchemaTranslator(false));
            } catch (PulsarAdminException e) {
                throw new CatalogException(
                        "Failed to create Pulsar admin with configuration:"
                                + catalogConfiguration.toString(),
                        e);
            }
        }
    }

    @Override
    public void close() throws CatalogException {
        if (catalogSupport != null) {
            catalogSupport.close();
            catalogSupport = null;
            LOG.info("Closed connection to Pulsar.");
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try {
            return catalogSupport.listDatabases();
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to list all databases in catalog: %s", getName()), e);
        }
    }

    @Override
    public CatalogDatabase getDatabase(String databaseName) throws CatalogException {
        try {
            return catalogSupport.getDatabase(databaseName);
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to get database info in catalog: %s", getName()), e);
        }
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        try {
            return catalogSupport.databaseExists(databaseName);
        } catch (PulsarAdminException e) {
            LOG.warn("Failed to check if database exists, encountered PulsarAdminError", e);
            return false;
        } catch (Exception e) {
            LOG.error("Failed to check if database exists", e);
            return false;
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        try {
            return catalogSupport.listTables(databaseName);
        } catch (PulsarAdminException.NotFoundException e) {
            throw new DatabaseNotExistException(getName(), databaseName, e);
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to list tables in database %s", databaseName), e);
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        try {
            return catalogSupport.getTable(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            throw new TableNotExistException(getName(), tablePath, e);
        } catch (PulsarAdminException | IncompatibleSchemaException e) {
            throw new CatalogException(
                    String.format("Failed to get table %s schema", tablePath.getFullName()), e);
        }
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return catalogSupport.tableExists(tablePath);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        } catch (PulsarAdminException e) {
            throw new CatalogException(
                    String.format("Failed to check table %s existence", tablePath.getFullName()),
                    e);
        }
    }

    // ------------------------------------------------------------------------
    // Unsupported catalog operations for Pulsar
    // There should not be such permission in the connector, it is very dangerous
    // ------------------------------------------------------------------------

    @Override
    public void createDatabase(String name, CatalogDatabase database, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterDatabase(String name, CatalogDatabase newDatabase, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(String name, boolean ignoreIfNotExists, boolean cascade)
            throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createTable(ObjectPath tablePath, CatalogBaseTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTable(
            ObjectPath tablePath, CatalogBaseTable newTable, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(ObjectPath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void renameTable(ObjectPath tablePath, String newTableName, boolean ignoreIfNotExists)
            throws TableNotExistException, TableAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(ObjectPath tablePath)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitions(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<CatalogPartitionSpec> listPartitionsByFilter(
            ObjectPath tablePath, List<Expression> expressions)
            throws TableNotExistException, TableNotPartitionedException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogPartition getPartition(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean partitionExists(ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition partition,
            boolean ignoreIfExists)
            throws TableNotExistException, TableNotPartitionedException,
                    PartitionSpecInvalidException, PartitionAlreadyExistsException,
                    CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropPartition(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec, boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartition(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition newPartition,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> listViews(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public List<String> listFunctions(String dbName)
            throws DatabaseNotExistException, CatalogException {
        return Collections.emptyList();
    }

    @Override
    public CatalogFunction getFunction(ObjectPath functionPath)
            throws FunctionNotExistException, CatalogException {
        return null;
    }

    @Override
    public boolean functionExists(ObjectPath functionPath) throws CatalogException {
        return false;
    }

    @Override
    public void createFunction(
            ObjectPath functionPath, CatalogFunction function, boolean ignoreIfExists)
            throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterFunction(
            ObjectPath functionPath, CatalogFunction newFunction, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropFunction(ObjectPath functionPath, boolean ignoreIfNotExists)
            throws FunctionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public CatalogTableStatistics getTableStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getTableColumnStatistics(ObjectPath tablePath)
            throws TableNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public CatalogTableStatistics getPartitionStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogTableStatistics.UNKNOWN;
    }

    @Override
    public CatalogColumnStatistics getPartitionColumnStatistics(
            ObjectPath tablePath, CatalogPartitionSpec partitionSpec)
            throws PartitionNotExistException, CatalogException {
        return CatalogColumnStatistics.UNKNOWN;
    }

    @Override
    public void alterTableStatistics(
            ObjectPath tablePath, CatalogTableStatistics tableStatistics, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterTableColumnStatistics(
            ObjectPath tablePath,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogTableStatistics partitionStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void alterPartitionColumnStatistics(
            ObjectPath tablePath,
            CatalogPartitionSpec partitionSpec,
            CatalogColumnStatistics columnStatistics,
            boolean ignoreIfNotExists)
            throws PartitionNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }
}
