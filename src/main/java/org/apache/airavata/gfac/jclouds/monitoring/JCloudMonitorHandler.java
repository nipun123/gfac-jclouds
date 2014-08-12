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
import org.apache.airavata.gfac.core.cpi.BetterGfacImpl;
import org.apache.airavata.gfac.core.cpi.GFacImpl;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.handler.ThreadedHandler;
import org.apache.airavata.gfac.core.monitor.MonitorID;
import org.jclouds.compute.domain.ExecResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public class JCloudMonitorHandler extends ThreadedHandler{
    private final static Logger logger= LoggerFactory.getLogger(JCloudMonitorHandler.class);

    private Monitor monitor;
    private ListenableFuture<ExecResponse> future;

    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {

        try{
            LinkedBlockingQueue<MonitorID> pushQueue = new LinkedBlockingQueue<MonitorID>();
            LinkedBlockingQueue<MonitorID> finishQueue = new LinkedBlockingQueue<MonitorID>();
            monitor=new Monitor(BetterGfacImpl.getMonitorPublisher(),pushQueue,finishQueue);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
       monitor.run();
    }

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException {
        super.invoke(jobExecutionContext);
        if(future ==null){
            logger.info("the future is not set for this job "+jobExecutionContext.getJobDetails().getJobID());
        }else{
            MonitorID monitorID=new JCloudsMonitorID(jobExecutionContext, future);
            monitorID.setJobID(jobExecutionContext.getJobDetails().getJobID());
            monitorID.setTaskID(jobExecutionContext.getTaskData().getTaskID());
            monitorID.setExperimentID(jobExecutionContext.getExperimentID());
            monitorID.setWorkflowNodeID(jobExecutionContext.getWorkflowNodeDetails().getNodeInstanceId());
            monitor.getRunningQueue().add(monitorID);
            future =null;
        }

    }

    public Monitor getMonitor() {
        return monitor;
    }

    public void setMonitor(Monitor monitor) {
        this.monitor = monitor;
    }

    public ListenableFuture<ExecResponse> getFuture() {
        return future;
    }

    public void setFuture(ListenableFuture<ExecResponse> future) {
        this.future = future;
    }

}

