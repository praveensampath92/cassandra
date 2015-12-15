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

import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;

public class OffendingQueriesMetric implements Comparable<OffendingQueriesMetric> {
    final Timer latencies;
    final Histogram memoryAllocation;
    final Counter totalLatencyMs;
    final MetricNameFactory factory;
    final String query;
    final String latencyMetricName;
    final String totalLatencyMetricName;
    final String allocationMetricName;

    public OffendingQueriesMetric(MetricNameFactory factory, String query) {
        this.query = query;
        this.factory = factory;

        allocationMetricName = query +".allocationMB";
        latencyMetricName = query + ".latencySeconds";
        totalLatencyMetricName = query + ".totalLatencyMs";
        latencies = Metrics.newTimer(factory.createMetricName(latencyMetricName), TimeUnit.SECONDS, TimeUnit.SECONDS);
        memoryAllocation = Metrics.newHistogram(factory.createMetricName(allocationMetricName));
        totalLatencyMs = Metrics.newCounter(factory.createMetricName(totalLatencyMetricName));
    }

    public void update(long memoryAllocatedMB, long latencyNanoSeconds) {
        latencies.update(latencyNanoSeconds, TimeUnit.NANOSECONDS);
        memoryAllocation.update(memoryAllocatedMB);
        totalLatencyMs.inc(latencyNanoSeconds / (1000 * 1000));
    }

    public int hashCode()
    {
        return query.hashCode();
    }

    public boolean equals (OffendingQueriesMetric secondMetric)
    {
        return secondMetric.query.equals(query);
    }

    public String getQuery()
    {
        return query;
    }

    public int compareTo(OffendingQueriesMetric secondMetric)
    {
        if (memoryAllocation.count() < secondMetric.memoryAllocation.count()) {
            return -1;
        }
        else if (memoryAllocation.count() > secondMetric.memoryAllocation.count()
                   || !query.equals(secondMetric.query)) {
            // ties can be broken arbitrarily for now
            return 1;
        } else
        {
            return 0;
        }
    }

    public void release() {
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(allocationMetricName));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(latencyMetricName));
        Metrics.defaultRegistry().removeMetric(factory.createMetricName(totalLatencyMetricName));
    }
}
