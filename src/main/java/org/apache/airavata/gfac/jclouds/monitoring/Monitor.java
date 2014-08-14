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

package org.apache.airavata.gfac.jclouds.monitoring;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.monitor.JobIdentity;
import org.apache.airavata.gfac.core.monitor.MonitorID;
import org.apache.airavata.common.utils.MonitorPublisher;
import org.apache.airavata.gfac.core.monitor.TaskIdentity;
import org.apache.airavata.gfac.core.monitor.state.JobStatusChangeRequest;
import org.apache.airavata.gfac.core.monitor.state.TaskStatusChangeRequest;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.monitor.core.AiravataAbstractMonitor;
import org.apache.airavata.model.workspace.experiment.JobDetails;
import org.apache.airavata.model.workspace.experiment.JobState;
import org.apache.airavata.model.workspace.experiment.TaskState;
import org.jclouds.compute.domain.ExecResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.BlockingQueue;

public class Monitor extends AiravataAbstractMonitor {
    private static final Logger log = LoggerFactory.getLogger(Monitor.class);

    private BlockingQueue<MonitorID> runningQueue;
    private BlockingQueue<MonitorID> finishQueue;
    private MonitorPublisher publisher;
    private boolean isMonitoring;

    public Monitor(MonitorPublisher publisher,BlockingQueue<MonitorID> runningQueue,
                   BlockingQueue<MonitorID> finishQueue){
        this.publisher = publisher;
        this.runningQueue = runningQueue;
        this.finishQueue = finishQueue;
    }

    public void run(){

        isMonitoring=true;
        while(isMonitoring){
            try {
                if(runningQueue.size()!=0){
                    MonitorID monitorID=runningQueue.take();
                    registerListener(monitorID);
                }
                monitorQueuedJobs();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try{
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void registerListener(MonitorID monitorID){
        finishQueue.add(monitorID);
    }

    public void monitorQueuedJobs(){

            Iterator<MonitorID> iterator = finishQueue.iterator();
            MonitorID next = null;
            while(iterator.hasNext()){
                next = iterator.next();
                if(next.getStatus()== JobState.COMPLETE){
                    JobExecutionContext jobExecutionContext=next.getJobExecutionContext();
                    try {
                        jobExecutionContext.getGfac().invokeOutFlowHandlers(jobExecutionContext);
                        JobDetails details=next.getJobExecutionContext().getJobDetails();
                        details.setJobDescription("job completed");
                        GFacUtils.saveJobStatus(jobExecutionContext, details, JobState.COMPLETE);

                        JobStatusChangeRequest jobStatusChangeRequest=new JobStatusChangeRequest(next);
                        publisher.publish(jobStatusChangeRequest);
                        publisher.publish(new TaskStatusChangeRequest(new TaskIdentity(next.getExperimentID(), next.getWorkflowNodeID(),
                                next.getTaskID()), TaskState.COMPLETED));
                    } catch (GFacException e) {
                        log.error("Error occurred while output handling");
                        publisher.publish(new TaskStatusChangeRequest(new TaskIdentity(next.getExperimentID(), next.getWorkflowNodeID(),
                                next.getTaskID()), TaskState.FAILED));
                    }
                    finishQueue.remove(next);
                    break;
                }else if(next.getStatus()== JobState.FAILED){
                    finishQueue.remove(next);
                    publisher.publish(new TaskStatusChangeRequest(new TaskIdentity(next.getExperimentID(), next.getWorkflowNodeID(),
                            next.getTaskID()), TaskState.FAILED));
                }
            }
    }

    public BlockingQueue<MonitorID> getRunningQueue() {
        return runningQueue;
    }

    public BlockingQueue<MonitorID> getFinishQueue() {
        return finishQueue;
    }

    public MonitorPublisher getPublisher() {
        return publisher;
    }

    public boolean isMonitoring() {
        return isMonitoring;
    }

    public void setMonitoring(boolean isMonitoring) {
        this.isMonitoring = isMonitoring;
    }

}

