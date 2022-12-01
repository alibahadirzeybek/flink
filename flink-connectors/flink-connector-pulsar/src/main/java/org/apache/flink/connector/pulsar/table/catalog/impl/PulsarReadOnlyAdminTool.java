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

import org.apache.flink.connector.pulsar.table.catalog.PulsarCatalogConfiguration;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.impl.schema.BytesSchema;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.schema.SchemaInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createAdmin;

/** A class that wraps Pulsar Admin API. */
public class PulsarReadOnlyAdminTool implements AutoCloseable {

    // system topics are not filtered out by default in Pulsar 2.10.0
    // this filter is incomplete and should be replaced by SystemTopicNames class
    // after 2.10.1 released.
    private static final String SYSTEM_TOPIC_PREFIX = "__";

    private final PulsarAdmin admin;

    public PulsarReadOnlyAdminTool(PulsarCatalogConfiguration catalogConfiguration) {
        this.admin = createAdmin(catalogConfiguration);
    }

    @Override
    public void close() {
        admin.close();
    }

    public List<String> listNamespaces() throws PulsarAdminException {
        List<String> tenants = admin.tenants().getTenants();
        List<String> namespaces = new ArrayList<String>();
        for (String tenant : tenants) {
            namespaces.addAll(admin.namespaces().getNamespaces(tenant));
        }
        return namespaces;
    }

    public boolean namespaceExists(String ns) throws PulsarAdminException {
        try {
            admin.namespaces().getTopics(ns);
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
        return true;
    }

    public List<String> getTopics(String ns) throws PulsarAdminException {
        List<String> nonPartitionedTopics = getNonPartitionedTopics(ns);
        List<String> partitionedTopics = admin.topics().getPartitionedTopicList(ns);
        List<String> allTopics = new ArrayList<>();
        Stream.of(partitionedTopics, nonPartitionedTopics).forEach(allTopics::addAll);
        return allTopics.stream()
                .map(t -> TopicName.get(t).getLocalName())
                .filter(topic -> !topic.startsWith(SYSTEM_TOPIC_PREFIX))
                .collect(Collectors.toList());
    }

    public boolean topicExists(String topicName) throws PulsarAdminException {
        try {
            PartitionedTopicMetadata partitionedTopicMetadata =
                    admin.topics().getPartitionedTopicMetadata(topicName);
            if (partitionedTopicMetadata.partitions > 0) {
                return true;
            }
        } catch (PulsarAdminException.NotFoundException e) {
            return false;
        }
        return false;
    }

    public SchemaInfo getPulsarSchema(String topic) {
        try {
            return admin.schemas().getSchemaInfo(TopicName.get(topic).toString());
        } catch (PulsarAdminException e) {
            if (e.getStatusCode() == 404) {
                return BytesSchema.of().getSchemaInfo();
            } else {
                throw new IllegalStateException(
                        String.format(
                                "Failed to get schema information for %s",
                                TopicName.get(topic).toString()),
                        e);
            }
        } catch (Throwable e) {
            throw new IllegalStateException(
                    String.format(
                            "Failed to get schema information for %s",
                            TopicName.get(topic).toString()),
                    e);
        }
    }

    private List<String> getNonPartitionedTopics(String namespace) throws PulsarAdminException {
        return admin.topics().getList(namespace).stream()
                .filter(t -> !TopicName.get(t).isPartitioned())
                .collect(Collectors.toList());
    }
}
