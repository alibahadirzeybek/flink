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

package org.apache.flink.connector.pulsar.table.catalog.impl;

import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.table.PulsarTableFactory;
import org.apache.flink.connector.pulsar.table.PulsarTableOptions;
import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogConfiguration;
import org.apache.flink.formats.json.JsonFormatFactory;
import org.apache.flink.formats.raw.RawFormatFactory;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogDatabaseImpl;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.VALUE_FORMAT;

/**
 * This class is the implementation layer of catalog operations. It uses {@link PulsarAdminTool} to
 * interact with Pulsar topics and manipulates metadata. {@link PulsarReadOnlyCatalogSupport}
 * distinguish between explicit and native tables.
 */
public class PulsarReadOnlyCatalogSupport {

    PulsarCatalogConfiguration catalogConfiguration;

    private final PulsarReadOnlyAdminTool pulsarAdminTool;

    private final SchemaTranslator schemaTranslator;

    public PulsarReadOnlyCatalogSupport(
            PulsarCatalogConfiguration catalogConfiguration, SchemaTranslator schemaTranslator)
            throws PulsarAdminException {
        this.catalogConfiguration = catalogConfiguration;
        this.pulsarAdminTool = new PulsarReadOnlyAdminTool(catalogConfiguration);
        this.schemaTranslator = schemaTranslator;
    }

    public List<String> listDatabases() throws PulsarAdminException {
        List<String> databases = new ArrayList<>();
        for (String ns : pulsarAdminTool.listNamespaces()) {
            if (!ns.startsWith("pulsar") && !ns.startsWith("public") && !ns.startsWith("__")) {
                // pulsar tenant/namespace mapped database
                databases.add(ns);
            }
        }
        return databases;
    }

    public boolean databaseExists(String name) throws PulsarAdminException {
        return pulsarAdminTool.namespaceExists(name);
    }

    public CatalogDatabase getDatabase(String name) throws PulsarAdminException {
        return new CatalogDatabaseImpl(new HashMap<>(), null);
    }

    public List<String> listTables(String name) throws PulsarAdminException {
        return pulsarAdminTool.getTopics(name);
    }

    public boolean tableExists(ObjectPath tablePath) throws PulsarAdminException {
        return pulsarAdminTool.topicExists(findTopicForNativeTable(tablePath));
    }

    public CatalogTable getTable(ObjectPath tablePath) throws PulsarAdminException {
        String existingTopic = findTopicForNativeTable(tablePath);
        final SchemaInfo pulsarSchema = pulsarAdminTool.getPulsarSchema(existingTopic);
        return schemaToCatalogTable(pulsarSchema, existingTopic);
    }

    private CatalogTable schemaToCatalogTable(SchemaInfo pulsarSchema, String topicName) {
        final Schema schema = schemaTranslator.pulsarSchemaToFlinkSchema(pulsarSchema);

        Map<String, String> initialTableOptions = new HashMap<>();
        initialTableOptions.put(PulsarTableOptions.TOPICS.key(), topicName);
        initialTableOptions.put(FactoryUtil.FORMAT.key(), JsonFormatFactory.IDENTIFIER);

        Map<String, String> enrichedTableOptions =
                fillDefaultOptionsFromCatalogOptions(initialTableOptions);

        return CatalogTable.of(schema, "", Collections.emptyList(), enrichedTableOptions);
    }

    // enrich table properties with proper catalog configs
    private Map<String, String> fillDefaultOptionsFromCatalogOptions(
            final Map<String, String> tableOptions) {
        Map<String, String> enrichedTableOptions = new HashMap<>();
        enrichedTableOptions.put(FactoryUtil.CONNECTOR.key(), PulsarTableFactory.IDENTIFIER);
        enrichedTableOptions.put(
                PulsarTableOptions.ADMIN_URL.key(),
                catalogConfiguration.get(PulsarOptions.PULSAR_ADMIN_URL));
        enrichedTableOptions.put(
                PulsarTableOptions.SERVICE_URL.key(),
                catalogConfiguration.get(PulsarOptions.PULSAR_SERVICE_URL));

        String authPlugin = catalogConfiguration.get(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME);
        if (authPlugin != null && !authPlugin.isEmpty()) {
            enrichedTableOptions.put(PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME.key(), authPlugin);
        }

        String authParams = catalogConfiguration.get(PulsarOptions.PULSAR_AUTH_PARAMS);
        if (authParams != null && !authParams.isEmpty()) {
            enrichedTableOptions.put(PulsarOptions.PULSAR_AUTH_PARAMS.key(), authParams);
        }

        // we always provide RAW format as a default format
        if (!tableOptions.containsKey(FactoryUtil.FORMAT.key())
                && !tableOptions.containsKey(VALUE_FORMAT.key())) {
            enrichedTableOptions.put(VALUE_FORMAT.key(), RawFormatFactory.IDENTIFIER);
        }

        if (!tableOptions.isEmpty()) {
            // table options could overwrite the default options provided above
            enrichedTableOptions.putAll(tableOptions);
        }
        return enrichedTableOptions;
    }

    private String findTopicForNativeTable(ObjectPath objectPath) {
        String database = objectPath.getDatabaseName();
        String topic = objectPath.getObjectName();

        NamespaceName ns = NamespaceName.get(database);
        TopicName fullName = TopicName.get(TopicDomain.persistent.toString(), ns, topic);
        return fullName.toString();
    }

    public void close() {
        if (pulsarAdminTool != null) {
            pulsarAdminTool.close();
        }
    }
}
