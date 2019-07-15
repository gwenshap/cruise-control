/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.common;

import com.linkedin.kafka.cruisecontrol.config.KafkaCruiseControlConfig;
import com.linkedin.kafka.cruisecontrol.monitor.MonitorUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MetadataClient {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataClient.class);
  private final AtomicInteger _metadataGeneration;
  private final Time _time;
  private final long _metadataTTL;
  private final long _refreshMetadataTimeout;
  private long _lastUpdateTime = 0;
  private Cluster _clusterCache;
  private AdminClient _adminClient;


//TODO: Unit tests
  public MetadataClient(KafkaCruiseControlConfig config,
                        long metadataTTL,
                        Time time) {
    _metadataGeneration = new AtomicInteger(0);
    _refreshMetadataTimeout = config.getLong(KafkaCruiseControlConfig.METADATA_MAX_AGE_CONFIG);
    _time = time;
    _adminClient = AdminClient.create(config.originals());
    _metadataTTL = metadataTTL;
    _clusterCache = Cluster.empty();
  }

  /**
   * Refresh the metadata. The method is synchronized because the network client is not thread safe.
   */
  public synchronized ClusterAndGeneration refreshMetadata() {
    return refreshMetadata(_refreshMetadataTimeout);
  }

  /**
   * Refresh the metadata.
   */
  public ClusterAndGeneration refreshMetadata(long timeout) {
    // Do not update metadata if the metadata has just been refreshed.
    if (_time.milliseconds() >= _lastUpdateTime + _metadataTTL) {
      //TODO: We need to make sure this isn't mutated while we are copying
      Cluster beforeUpdate = copyCluster(_clusterCache);

      // Cruise Control always fetch metadata for all the topics.

      //TODO: Cleanup exception handling
      //TODO: Do I need to block here?
      //TODO: I'm blocking with same timeout multiple times
      try {

        Set<String> topics = _adminClient.listTopics().names().get(timeout, TimeUnit.MILLISECONDS);
        Collection<Node> nodes = _adminClient.describeCluster().nodes().get(timeout,
                TimeUnit.MILLISECONDS);
        Map<String, TopicDescription> describeTopicResult =
                _adminClient.describeTopics(topics).all().get(timeout, TimeUnit.MILLISECONDS);
        _clusterCache = topicDescriptionsToCluster(nodes, describeTopicResult);

        if (LOG.isDebugEnabled()) {
          LOG.debug("Updated metadata {}", _clusterCache);
        }
      } catch (ExecutionException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        LOG.warn("Failed to update metadata in {}ms. Using old metadata with last successful update {}.",
                timeout, _lastUpdateTime);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      if (MonitorUtils.metadataChanged(beforeUpdate, _clusterCache)) {
        _metadataGeneration.incrementAndGet();
      }
    }

    return new ClusterAndGeneration(_clusterCache, _metadataGeneration.get());
  }

  /**
   * Close the metadata client. Synchronized to avoid interrupting the network client during a poll.
   */
  public synchronized void close() {
    if (_adminClient != null) {
      _adminClient.close();
    }
  }

  /**
   * Get the current cluster and generation
   */
  public ClusterAndGeneration clusterAndGeneration() {
    return new ClusterAndGeneration(cluster(), _metadataGeneration.get());
  }

  /**
   * Get the current cluster.
   */
  public Cluster cluster() {
    return _clusterCache;
  }

  public static class ClusterAndGeneration {
    private final Cluster _cluster;
    private final int _generation;

    public ClusterAndGeneration(Cluster cluster, int generation) {
      _cluster = cluster;
      _generation = generation;
    }

    public Cluster cluster() {
      return _cluster;
    }

    public int generation() {
      return _generation;
    }
  }

  private Cluster copyCluster(Cluster cluster) {
    // A bit of a hack, but creating a new Cluster by merging the old one with an empty
    // partition map is the easiest way to copy a cluster.

    return cluster.withPartitions(Collections.emptyMap());

  }

  //TODO: need renaming
  private Cluster topicDescriptionsToCluster(Collection<Node> nodes,
                                             Map<String, TopicDescription> describeTopicResult) {
    List<PartitionInfo> partitionInfos = new LinkedList<>();

    for (Map.Entry<String, TopicDescription> topicDescription: describeTopicResult.entrySet()) {
      for (TopicPartitionInfo topicPartitionInfo: topicDescription.getValue().partitions()) {
        PartitionInfo partitionInfo = new PartitionInfo(topicDescription.getKey(),
                topicPartitionInfo.partition(),
                topicPartitionInfo.leader(),
                topicPartitionInfo.replicas().stream().toArray(Node[]::new),
                topicPartitionInfo.isr().stream().toArray(Node[]::new));
      partitionInfos.add(partitionInfo);
      }
    }
    return new Cluster("cached cluster metadata", nodes, partitionInfos, Collections.emptySet(),
            Collections.emptySet());

  }
}





