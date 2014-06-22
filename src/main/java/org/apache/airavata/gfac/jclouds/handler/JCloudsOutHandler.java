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
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.handler.AbstractHandler;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.apache.airavata.gfac.jclouds.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.jclouds.utils.JCloudsUtils;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.apache.airavata.schemas.gfac.ApplicationDeploymentDescriptionType;
import org.apache.airavata.schemas.gfac.Ec2HostType;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Set;

public class JCloudsOutHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(JCloudsOutHandler.class);
    private JCloudsUtils jCloudsUtils;
    private LoginCredentials credentials;
    private JCloudsFileTransfer fileTransfer;
    private JCloudsSecurityContext securityContext;

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        jCloudsUtils=new JCloudsUtils();
        try{
            securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        }catch (GFacException e){
            throw new GFacHandlerException("Error while reading security context");
        }
        fileTransfer=new JCloudsFileTransfer(jCloudsUtils.getContext(),securityContext.getNodeId(),jCloudsUtils.getCredentials());

        if(jobExecutionContext.getApplicationContext().getHostDescription().getType() instanceof Ec2HostType){
            TaskDetails taskDetails=null;
            try{
                taskDetails=(TaskDetails)jobExecutionContext.getRegistry().get(RegistryModelType.TASK_DETAIL,jobExecutionContext.getTaskData().getTaskID());
            }catch (RegistryException e){
                log.error("Error retrieving job details from Registry");
                throw new GFacHandlerException("Error retrieving job details from Registry", e);
            }

        }
        super.invoke(jobExecutionContext);
        DataTransferDetails detail = new DataTransferDetails();
        TransferStatus status = new TransferStatus();

        try{
            MessageContext outMessage=jobExecutionContext.getOutMessageContext();
            Set<String> outputs=outMessage.getParameters().keySet();
            for(String output:outputs){
                ActualParameter actualParameter=(ActualParameter)outMessage.getParameters().get(output);
                if ("URI".equals(actualParameter.getType().getType().toString())){


                }
                status.setTransferState(TransferState.DOWNLOAD);
                detail.setTransferStatus(status);
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
            }
        }catch (Exception e){
            try {
                status.setTransferState(TransferState.FAILED);
                detail.setTransferStatus(status);
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
                GFacUtils.saveErrorDetails(jobExecutionContext, e.getLocalizedMessage(), CorrectiveAction.CONTACT_SUPPORT, ErrorCategory.FILE_SYSTEM_FAILURE);
            } catch (Exception e1) {
                throw new GFacHandlerException("Error persisting status", e1, e1.getLocalizedMessage());
            }
            throw new GFacHandlerException("Error in retrieving results", e);
        }
    }

    public String stageOutputFiles(String remoteFile,String localFile){
       return null;
    }

    public void stages3Files(JobExecutionContext jobExecutionContext,String paramValue){
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
        int i=paramValue.lastIndexOf("/");
        String fileName=paramValue.substring(i+1);
        String targetFile=null;

        try{
            targetFile=app.getOutputDataDirectory()+"/"+fileName;
            if (!jCloudsUtils.isS3CmdInstall(credentials)){
                jCloudsUtils.installS3CmdOnNode(securityContext,credentials);
            }
            String command="s3cmd put "+targetFile+" "+paramValue;
            ExecResponse response=jCloudsUtils.runScriptOnNode(credentials, command, false);
            int exitStatus=response.getExitStatus();
            if(exitStatus ==0){
              log.info("Sucessfully put the file "+targetFile+" to s3 ");
            }else{
              log.info("fail to put the file "+targetFile+" to s3");
            }
        }catch (Exception e){
            log.error("Error while puting file to s3");
        }

    }

    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {
    }
}
