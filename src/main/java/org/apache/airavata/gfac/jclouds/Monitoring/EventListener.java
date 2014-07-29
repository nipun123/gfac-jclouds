package org.apache.airavata.gfac.jclouds.Monitoring;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.cpi.GFacImpl;
import org.apache.airavata.gfac.core.monitor.MonitorID;
import org.apache.airavata.model.workspace.experiment.JobState;
import org.jclouds.compute.domain.ExecResponse;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

/**
 * Created by root on 7/21/14.
 */
public class EventListener implements Runnable {

    private static final org.slf4j.Logger log = LoggerFactory.getLogger(EventListener.class);

    MonitorID monitorID;
    ListenableFuture<ExecResponse> future;

    public EventListener(MonitorID monitorID,ListenableFuture<ExecResponse> future){
        this.monitorID=monitorID;
        this.future=future;
    }

    @Override
    public void run() {
        handleFutureCall();
    }

    private void handleFutureCall(){
       JCloudsMonitorID jCloudsMonitorID=(JCloudsMonitorID)monitorID;
        try {
            if(future.isCancelled()){
               log.info("future is cancelled cannot listen to the Job Events");
            }else{

                if(future.isDone()){
                    ExecResponse response=future.get();
                    int status=response.getExitStatus();

                    if(status==0){
                      monitorID.setStatus(JobState.COMPLETE);
                    }else if(status==127){
                      monitorID.setStatus(JobState.FAILED);
                    }
                }else{
                    log.info("the job is not done");
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}

