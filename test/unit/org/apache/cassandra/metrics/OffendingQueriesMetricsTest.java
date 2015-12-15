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

import java.util.LinkedList;

import org.junit.Test;

import com.yammer.metrics.core.MetricName;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.IndexExpression;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class OffendingQueriesMetricsTest
{
    @Test
    public void testReplacement() {
        final MetricNameFactory factory = new MetricNameFactory()
        {
            public MetricName createMetricName(String metricName)
            {
                return new MetricName(LatencyMetrics.class, "abc."+metricName);
            }
        };

        final OffendingQueriesMetrics metrics = new OffendingQueriesMetrics(factory, 2);
        final long MegaBytes = 1024L *1024L;
        final long NanosInMillis = 1000L * 1000L;
        IndexExpression e1 = new IndexExpression(
            ByteBufferUtil.bytes("abc"), Operator.EQ, ByteBufferUtil.bytes("abc1"));
        IndexExpression e2 = new IndexExpression(
            ByteBufferUtil.bytes("abc"), Operator.EQ, ByteBufferUtil.bytes("abc1"));
        IndexExpression e3 = new IndexExpression(
            ByteBufferUtil.bytes("abc"), Operator.EQ, ByteBufferUtil.bytes("abc1"));
        LinkedList<IndexExpression> c1 = new LinkedList<IndexExpression>();
        LinkedList<IndexExpression> c2 = new LinkedList<IndexExpression>();
        LinkedList<IndexExpression> c3 = new LinkedList<IndexExpression>();

        c1.add(e1);
        c2.add(e2);
        c3.add(e3);

        String keyspaceName = "ks1";

        metrics.update(keyspaceName, "cf1", c1, 121 * MegaBytes, 1231 * NanosInMillis);
        metrics.update(keyspaceName, "cf2", c2, 123 * MegaBytes, 1234 * NanosInMillis);
        metrics.update(keyspaceName, "cf1", c1, 123 * MegaBytes, 1233 * NanosInMillis);
        metrics.update(keyspaceName, "cf3", c3, 123 * MegaBytes, 1234 * NanosInMillis);

        String metric1Name = metrics.getMetricName(keyspaceName, "cf1", c1);
        String metric2Name = metrics.getMetricName(keyspaceName, "cf2", c2);
        String metric3Name = metrics.getMetricName(keyspaceName, "cf3", c3);

        assertFalse(metrics.metricMap.containsKey(metrics.getMetricName(keyspaceName, "cf2", c2)));
        assertTrue(metrics.metricMap.size() == 2);
        assertTrue(metrics.offendingQueriesMetrics.peek().getQuery().equals(metric3Name));
        assertTrue(metrics.metricMap.get(metric1Name).latencies.mean() == 1.232);
        assertTrue(metrics.metricMap.get(metric1Name).memoryAllocation.mean() == 122);
        assertTrue(metrics.metricMap.get(metric3Name).latencies.mean() == 1.234);
        assertTrue(metrics.metricMap.get(metric3Name).memoryAllocation.mean() == 123);
    }
}
