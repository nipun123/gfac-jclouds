/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

package org.apache.airavata.gfac.jclouds.Monitoring;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.monitor.MonitorID;
import org.jclouds.compute.domain.ExecResponse;

public class JCloudsMonitorID extends MonitorID{
    private ListenableFuture<ExecResponse> future;

    public JCloudsMonitorID(JobExecutionContext jobExecutionContext,ListenableFuture future){
        super(jobExecutionContext);
        this.future=future;
    }

    public ListenableFuture<ExecResponse> getFuture() {
        return future;
    }

    public void setFuture(ListenableFuture<ExecResponse> future) {
        this.future = future;
    }
}
