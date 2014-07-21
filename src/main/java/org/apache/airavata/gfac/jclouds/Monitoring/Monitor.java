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

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.airavata.gfac.core.monitor.MonitorID;
import org.apache.airavata.gfac.core.notification.MonitorPublisher;
import org.apache.airavata.gfac.monitor.core.AiravataAbstractMonitor;
import org.jclouds.compute.domain.ExecResponse;

import java.util.concurrent.BlockingQueue;

public class Monitor extends AiravataAbstractMonitor {

    private BlockingQueue<MonitorID> runningQueue;

    private BlockingQueue<MonitorID> finishQueue;
    private MonitorPublisher publisher;
    private boolean isMonitoring;

    public Monitor(MonitorPublisher publisher,BlockingQueue<MonitorID> runningQueue,
                   BlockingQueue<MonitorID> finishQueue){
        this.publisher = publisher;
        this.runningQueue = runningQueue;
        this.finishQueue = finishQueue;
        this.publisher = new MonitorPublisher(new EventBus());
        this.publisher.registerListener(this);

    }

    public void run(){

        isMonitoring=true;
        while(isMonitoring){
            System.out.println(" execute run method of Jclouds Monitor");
            try {
                if(runningQueue.size()!=0){
                    MonitorID monitorID=runningQueue.take();
                    registerListener(monitorID);
                }
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
        JCloudsMonitorID jCloudsMonitorID=(JCloudsMonitorID)monitorID;
        ListenableFuture<ExecResponse> future=jCloudsMonitorID.getFuture();

        future.addListener(new EventListener(monitorID,future), MoreExecutors.sameThreadExecutor());

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

