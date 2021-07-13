/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.commitpolicy;

import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.List;
import java.util.Map;

/**
 * Utils to for Hive-backed table.
 */
public class HiveTableUtil {

    private HiveTableUtil() {
    }

    // --------------------------------------------------------------------------------------------
    //  Helper methods
    // --------------------------------------------------------------------------------------------

    /**
     * Creates a Hive partition instance.
     */
    public static Partition createHivePartition(String dbName,
                                                String tableName,
                                                List<String> values,
                                                StorageDescriptor sd,
                                                Map<String, String> parameters) {
        Partition partition = new Partition();
        partition.setDbName(dbName);
        partition.setTableName(tableName);
        partition.setValues(values);
        partition.setParameters(parameters);
        partition.setSd(sd);
        int currentTime = (int) (System.currentTimeMillis() / 1000);
        partition.setCreateTime(currentTime);
        partition.setLastAccessTime(currentTime);
        return partition;
    }

}
