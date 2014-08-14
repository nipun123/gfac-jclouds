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
package org.apache.airavata.gfac.jclouds.provider.impl;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.MappingFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.cpi.BetterGfacImpl;
import org.apache.airavata.gfac.core.cpi.GFacImpl;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.handler.ThreadedHandler;
import org.apache.airavata.gfac.core.notification.events.StartExecutionEvent;
import org.apache.airavata.gfac.core.provider.AbstractProvider;
import org.apache.airavata.gfac.core.provider.GFacProviderException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.jclouds.monitoring.EventListener;
import org.apache.airavata.gfac.jclouds.monitoring.JCloudMonitorHandler;
import org.apache.airavata.gfac.jclouds.monitoring.JCloudsMonitorID;
import org.apache.airavata.gfac.jclouds.exceptions.PublicKeyException;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.apache.airavata.gfac.jclouds.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.jclouds.utils.JCloudsUtils;
import org.apache.airavata.model.workspace.experiment.JobState;
import org.apache.airavata.schemas.gfac.Ec2ApplicationDeploymentType;

import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.LoginCredentials;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

public class JCloudsProvider extends AbstractProvider {
    private static final Logger log = LoggerFactory.getLogger(JCloudsProvider.class);

    private String jobID = null;
    private LoginCredentials credentials;
    private JCloudsUtils jCloudsUtils;
    private ComputeServiceContext context;
    private JCloudsSecurityContext securityContext;

    public void initialize(JobExecutionContext jobExecutionContext) throws GFacException, GFacProviderException {
        if(jobExecutionContext!=null) {
            jobID=jobExecutionContext.getTaskData().getTaskID()+jobExecutionContext.getApplicationContext().getHostDescription().getType().getHostAddress()+"_"+ Calendar.getInstance().getTimeInMillis();
        }else{
            log.error("Job Execution context is null");
            throw new GFacProviderException("Job Execution Context is null" + jobExecutionContext);
        }
        super.initialize(jobExecutionContext);
        jCloudsUtils=new JCloudsUtils();
        securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        context=securityContext.getContext();
        try {
            credentials=jCloudsUtils.makeLoginCredentials(securityContext);
        } catch (PublicKeyException e) {
            log.error("Fail to create credentials for ec2 node");
            throw new GFacException("Fail to create credentials for ec2 node");
        }

        details.setJobID(jobID);
        details.setJobDescription("Job is submitted");
        jobExecutionContext.setJobDetails(details);
        GFacUtils.saveJobStatus(jobExecutionContext, details, JobState.SETUP);
    }


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
        String command=buildCommand(jobExecutionContext);
        jobExecutionContext.getNotifier().publish(new StartExecutionEvent());

        try{
            ListenableFuture<ExecResponse> future=jCloudsUtils.submitScriptToNode(context.getComputeService(),credentials,command,securityContext.getNodeId(),true);
            delegateToMonitorHandler(future);
        }catch (Exception e){
            log.error("Error submitting job "+e.toString());
            details.setJobID("none");
            GFacUtils.saveJobStatus(jobExecutionContext,details, JobState.FAILED);
            throw new GFacException("Failed to submit job "+jobID);

        }
        String jobStatusMessage = "submitted JobID= " + jobID;
        log.info(jobStatusMessage);
        GFacUtils.saveJobStatus(jobExecutionContext, details, JobState.SUBMITTED);
    }


    public String buildCommand(JobExecutionContext context){
        ApplicationDescription ec2App=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription();
        Ec2ApplicationDeploymentType app=(Ec2ApplicationDeploymentType)ec2App.getType();
        String SPACE=" ";
        StringBuffer cmd=new StringBuffer();
        String executableType="";
        if(app.getExecutableType()!=null){
            executableType=app.getExecutableType();
        }

        cmd.append("cd "+app.getScratchWorkingDirectory()+"\n");
        cmd.append(executableType);
        cmd.append(SPACE);
        cmd.append(app.getExecutableLocation());

        MessageContext input=context.getInMessageContext();
        Map<String, Object> inputs = input.getParameters();
        Set<String> keys = inputs.keySet();
        for(String paraName :keys){
            ActualParameter actualParameter=(ActualParameter)input.getParameter(paraName);
            String paramValue=MappingFactory.toString(actualParameter);
            cmd.append(SPACE);
            cmd.append(paramValue);
        }

        cmd.append(SPACE);
        cmd.append("1>");
        cmd.append(SPACE);
        cmd.append(app.getStandardOutput());
        cmd.append(SPACE);
        cmd.append("2>");
        cmd.append(SPACE);
        cmd.append(app.getStandardError());
        return cmd.toString();

    }

    public void delegateToMonitorHandler(ListenableFuture future){
        List<ThreadedHandler> handlers= BetterGfacImpl.getDaemonHandlers();
        ThreadedHandler monitorHandler=null;
        for(ThreadedHandler threadedHandler:handlers){
            if((threadedHandler.getClass().getName()).equals("org.apache.airavata.gfac.jclouds.Monitoring.JCloudMonitorHandler")){
                log.info("job launched successfully now parsing it to monitoring "+jobID);
                monitorHandler=threadedHandler;
            }
        }

        if(monitorHandler==null){
            log.info("No suitable handler exist to monitor job so out handler will not be invoked");
        }else{
            try {
                JCloudsMonitorID monitorID=new JCloudsMonitorID(jobExecutionContext,future);
                monitorID.setStatus(JobState.ACTIVE);
                future.addListener(new EventListener(monitorID,future), Executors.newSingleThreadExecutor());
                ((JCloudMonitorHandler)monitorHandler).setMonitorID(monitorID);
                monitorHandler.invoke(jobExecutionContext);
            } catch (GFacHandlerException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void dispose(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void cancelJob(String jobId, JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void initProperties(Map<String, String> properties) throws GFacProviderException, GFacException {
    }
}

