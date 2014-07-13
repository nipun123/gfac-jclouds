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
package org.apache.airavata.gfac.jclouds.handler;

import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.MappingFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.handler.AbstractHandler;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.jclouds.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.jclouds.utils.JCloudsUtils;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.schemas.gfac.ApplicationDeploymentDescriptionType;
import org.apache.airavata.schemas.gfac.URIParameterType;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;

import java.io.File;
import java.util.Properties;
import java.util.Set;

public class JCloudsInHandler extends AbstractHandler{
    private static final Logger log = LoggerFactory.getLogger(JCloudsInHandler.class);
    private JCloudsFileTransfer transfer;
    private JCloudsUtils jCloudsUtils;
    private LoginCredentials credentials;
    private JCloudsSecurityContext securityContext;

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        log.info("Invoking Handler");
        if (jobExecutionContext==null){
            throw new GFacHandlerException("JobExecution is null");
        }
        try{
           securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        } catch (GFacException e) {
           e.printStackTrace();
        }
        if (securityContext==null){
            throw new GFacHandlerException("security context is not properly set");
        }else{
            log.info("successfully retrived security context");
        }

        securityContext.getCredentialsFromStore();

        jCloudsUtils=JCloudsUtils.getInstance();
        try {
            jCloudsUtils.initJCloudsEnvironment(jobExecutionContext);
        }catch (GFacException e){
            throw new GFacHandlerException("fail to initialize ec2 environment");
        }
        credentials=jCloudsUtils.getCredentials();
        transfer=new JCloudsFileTransfer(jCloudsUtils.getContext(),securityContext.getNodeId(),credentials);
        log.info("Setup Job directories");
        super.invoke(jobExecutionContext);

        makeDirectory(jobExecutionContext);
        DataTransferDetails detail = new DataTransferDetails();
        TransferStatus status = new TransferStatus();
        MessageContext inputNew=new MessageContext();

        try{
           MessageContext inputs=jobExecutionContext.getInMessageContext();
           Set<String> parameters=inputs.getParameters().keySet();
           for(String paramName:parameters){
               ActualParameter actualParameter=(ActualParameter)inputs.getParameters().get(paramName);
               String paramValue=MappingFactory.toString(actualParameter);
               String paramValueNew="";
               if("URI".equals(actualParameter.getType().getType().toString())){
                    paramValueNew=stageInputFiles(jobExecutionContext,paramValue);
                   ((URIParameterType)actualParameter.getType()).setValue(paramValueNew);
                    detail.setTransferDescription("Input Data Staged: " + paramValueNew);
               }//else if("S3".equals(actualParameter.getType().getType().toString())){
                   // paramValueNew=stageS3Files(jobExecutionContext,paramValue);
                   // ((S3ParameterType)actualParameter.getType()).setValue(paramValueNew);
                   // detail.setTransferDescription("Input Data Staged: " + paramValueNew);
                   // need to add this when s3type add to gfac schema
                   
               //}
               inputNew.getParameters().put(paramName,actualParameter);
               status.setTransferState(TransferState.UPLOAD);
               detail.setTransferStatus(status);
               registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());

           }
        }catch (Exception e){
            log.error(e.getMessage());
            status.setTransferState(TransferState.FAILED);
            detail.setTransferStatus(status);
            try{
                GFacUtils.saveErrorDetails(jobExecutionContext, e.getLocalizedMessage(), CorrectiveAction.CONTACT_SUPPORT, ErrorCategory.FILE_SYSTEM_FAILURE);
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
            }catch (Exception e1){
                throw new GFacHandlerException("Error persisting status", e1, e1.getLocalizedMessage());
            }
            throw new GFacHandlerException("Error while input File Staging", e, e.getLocalizedMessage());
        }
        jobExecutionContext.setInMessageContext(inputNew);
    }

    private String stageInputFiles(JobExecutionContext jobExecutionContext,String paramValue) throws Exception{
       ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
       int i=paramValue.lastIndexOf("/");
       String fileName=paramValue.substring(i+1);
       String targetFile=null;
       try{
            targetFile=app.getInputDataDirectory()+"/"+fileName;
            transfer.uploadFileToEc2(targetFile,paramValue);
       }catch (Exception e){
            log.error("Error while uploading file "+paramValue+" :"+e.toString());
       }
       return targetFile;
    }

    private String stageS3Files(JobExecutionContext jobExecutionContext,String paramValue){
       ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
       int i=paramValue.lastIndexOf("/");
       String fileName=paramValue.substring(i+1);
       String targetFile=null;
       try {
            targetFile=app.getInputDataDirectory()+File.separator+fileName;
            if(!jCloudsUtils.isS3CmdInstall(credentials)){
                jCloudsUtils.installS3CmdOnNode(securityContext,credentials);
            }
            String command="s3cmd get "+paramValue+" "+targetFile;
            ExecResponse response=jCloudsUtils.runScriptOnNode(credentials,command,false);
            int exitStatus=response.getExitStatus();
            if (exitStatus==0){
              log.info("successfully get file "+targetFile+" from s3");
            }else{
              log.info("fail to get file "+targetFile+" from s3");
            }
       }catch (Exception e){
          log.error("Error while geting file from s3 "+e.toString());
       }
       return targetFile;
    }

    private void makeDirectory(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
        String inputDataDirectory=app.getInputDataDirectory();
        String outputDataDirectory=app.getOutputDataDirectory();
        String createDirectories=new StringBuilder().append("mkdir -m 777 "+inputDataDirectory+"\n")
                                                    .append("mkdir -m 777 "+outputDataDirectory+"\n").toString();
        ExecResponse response=jCloudsUtils.runScriptOnNode(credentials, createDirectories, true);

        try{
            if(response.getExitStatus()==0){
                DataTransferDetails detail = new DataTransferDetails();
                TransferStatus status = new TransferStatus();
                status.setTransferState(TransferState.DIRECTORY_SETUP);
                detail.setTransferStatus(status);
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
            }else if(response.getExitStatus()==1){
                log.info("input data directory and outputdata directory already exist");
            }
            else{
                DataTransferDetails detail = new DataTransferDetails();
                TransferStatus status = new TransferStatus();
                status.setTransferState(TransferState.FAILED);
                detail.setTransferStatus(status);
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
            }
        }catch (RegistryException re){
            throw new GFacHandlerException("Error persisting status");
        }

    }
    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {
    }
}
