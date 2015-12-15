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

package org.apache.cassandra.metrics;


import java.nio.charset.CharacterCodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yammer.metrics.core.MetricName;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.db.filter.ExtendedFilter;
import org.apache.cassandra.utils.ByteBufferUtil;

public class OffendingQueriesMetrics {
    Map<String, OffendingQueriesMetric> metricMap;
    PriorityQueue<OffendingQueriesMetric> offendingQueriesMetrics;
    OffendingMetricNameFactory factory;
    Logger logger = LoggerFactory.getLogger(OffendingQueriesMetrics.class);

    final int trackQueriesLimit;
    final static long memoryThresholdMB = 1;
    final static long latencyThresholdNanoSeconds = 1000L * 1000L * 1000L;

    class OffendingMetricNameFactory implements MetricNameFactory {
        MetricNameFactory factory;

        OffendingMetricNameFactory(MetricNameFactory factory) {
            this.factory = factory;
        }

        public MetricName createMetricName(String metricName)
        {
            return factory.createMetricName("OffendingQueries." + metricName);
        }
    }

    OffendingQueriesMetrics(MetricNameFactory factory, int trackQueriesLimit)
    {
        this.factory = new OffendingMetricNameFactory(factory);
        this.trackQueriesLimit = trackQueriesLimit;
        metricMap = new HashMap<>();
        offendingQueriesMetrics = new PriorityQueue<>(trackQueriesLimit);
    }


    String getMetricName(String ksName, String cfName, List<IndexExpression> clauses) {
        String metricName = ksName + "." + cfName + "._";
        for (IndexExpression ex: clauses) {
            try
            {
                metricName += ByteBufferUtil.string(ex.column) + "_" +
                            ex.operator.name();
            } catch (CharacterCodingException e) {
                logger.warn("Cannot encode the column or value for " + cfName, e);
            }
        }
        return metricName;
    }

    // should it be synchronized
    public synchronized void update(String ksName, String cfName,
                                    List<IndexExpression> clauses,
                                    long memoryAllocatedBytes,
                                    long latencyNanoSeconds)
    {
        long memoryAllocatedMB = memoryAllocatedBytes / (1024L * 1024L);

        // Early return if not really an offending query
        if (memoryAllocatedMB < memoryThresholdMB && latencyNanoSeconds < latencyThresholdNanoSeconds)
        {
            return;
        }

        String metricName = getMetricName(ksName, cfName, clauses);
        logger.warn("Offending query for " + cfName + ", Metric " + metricName + " with allocation"
                    + memoryAllocatedBytes + " and latency" + latencyNanoSeconds);
        if (metricMap.containsKey(metricName))
        {
            OffendingQueriesMetric metric = metricMap.get(metricName);
            metric.update(memoryAllocatedMB, latencyNanoSeconds);
            assert (offendingQueriesMetrics.remove(metric));
            offendingQueriesMetrics.add(metric);
        }
        else
        {
            OffendingQueriesMetric metric = new OffendingQueriesMetric(factory, metricName);
            metric.update(memoryAllocatedMB, latencyNanoSeconds);
            if (offendingQueriesMetrics.size() >= trackQueriesLimit)
            {
                OffendingQueriesMetric evictedMetric = offendingQueriesMetrics.peek();
                evictedMetric.release();
                metricMap.remove(evictedMetric.getQuery());
                assert (offendingQueriesMetrics.remove() == evictedMetric);
            }
            offendingQueriesMetrics.add(metric);
            metricMap.put(metricName, metric);
        }
    }
}
