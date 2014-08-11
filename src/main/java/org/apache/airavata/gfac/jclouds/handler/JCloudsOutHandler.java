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

import org.airavata.appcatalog.cpi.AppCatalogException;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.MappingFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.handler.AbstractHandler;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.core.utils.OutputUtils;
import org.apache.airavata.gfac.jclouds.exceptions.FileTransferException;
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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class JCloudsOutHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(JCloudsOutHandler.class);
    private JCloudsUtils jCloudsUtils;
    private LoginCredentials credentials;
    private JCloudsFileTransfer fileTransfer;
    private JCloudsSecurityContext securityContext;

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        jCloudsUtils=JCloudsUtils.getInstance();
        try{
            securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
            if(securityContext==null){
                jCloudsUtils.addSecurityContext(jobExecutionContext);
            }
            jCloudsUtils.initJCloudsEnvironment(jobExecutionContext);
        }catch (GFacException e){
            throw new GFacHandlerException("Error while reading security context or initialising ec2 environment");
        } catch (AppCatalogException e) {
            throw new GFacHandlerException("Error while adding security context");
        }
        credentials=jCloudsUtils.getCredentials();
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
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();

        String localStdOutFile;
        String localStdErrFile;
        String outputDataDir=File.separator+"tmp"+File.separator+jobExecutionContext.getExperimentID()+"-"+jobExecutionContext.getTaskData().getTaskID();
        (new File(outputDataDir)).mkdir();

        localStdOutFile=outputDataDir+File.separator+"stdout";
        localStdErrFile=outputDataDir+File.separator+"stderr";

        try{
            fileTransfer.downloadFilesFromEc2(localStdOutFile,app.getStandardOutput());
            fileTransfer.downloadFilesFromEc2(localStdErrFile,app.getStandardError());

            String stdoutStr=GFacUtils.readFileToString(localStdOutFile);
            String stderrStr=GFacUtils.readFileToString(localStdErrFile);

            status.setTransferState(TransferState.COMPLETE);
            detail.setTransferStatus(status);
            detail.setTransferDescription("STDOUT:" + stdoutStr);
            registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());

            status.setTransferState(TransferState.COMPLETE);
            detail.setTransferStatus(status);
            detail.setTransferDescription("STDERR:" + stderrStr);
            registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());

            List<DataObjectType> outputArray = new ArrayList<DataObjectType>();
            MessageContext outMessage=jobExecutionContext.getOutMessageContext();
            Set<String> outputs=outMessage.getParameters().keySet();
            List<String> outputFileList=getOutPutFileList(jobExecutionContext);
            int fileCount=0;
            for(String output:outputs){
                ActualParameter actualParameter=(ActualParameter)outMessage.getParameters().get(output);
                if ("URI".equals(actualParameter.getType().getType().toString())){
                   if(outputFileList.size()>fileCount){
                      String localFileName=outputDataDir+File.separator+outputFileList.get(fileCount);
                      stageOutputFiles(jobExecutionContext,localFileName);
                      fileCount++;

                      jobExecutionContext.addOutputFile(localFileName);
                      DataObjectType dataObjectType = new DataObjectType();
                      dataObjectType.setValue(localFileName);
                      dataObjectType.setKey(output);
                      dataObjectType.setType(DataType.URI);
                      outputArray.add(dataObjectType);
                   }
                }else if ("S3".equals(actualParameter.getType().getType().toString())){
                    stages3Files(jobExecutionContext,outputFileList.get(fileCount));
                }
                else{
                     DataObjectType out = new DataObjectType();
                     out.setKey(output);
                     out.setType(DataType.STRING);
                     out.setValue(stdoutStr);
                     outputArray.add(out);
                }
            }
            status.setTransferState(TransferState.DOWNLOAD);
            detail.setTransferStatus(status);
            detail.setTransferDescription(outputDataDir);
            registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
            registry.add(ChildDataType.EXPERIMENT_OUTPUT, outputArray, jobExecutionContext.getExperimentID());
        }catch (GFacHandlerException e){
            try {
                status.setTransferState(TransferState.FAILED);
                detail.setTransferStatus(status);
                detail.setTransferDescription("file down failed");
                registry.add(ChildDataType.DATA_TRANSFER_DETAIL, detail, jobExecutionContext.getTaskData().getTaskID());
                GFacUtils.saveErrorDetails(jobExecutionContext, e.getLocalizedMessage(), CorrectiveAction.CONTACT_SUPPORT, ErrorCategory.FILE_SYSTEM_FAILURE);
            } catch (Exception e1) {
                throw new GFacHandlerException("Error persisting error details", e1, e1.getLocalizedMessage());
            }
            throw new GFacHandlerException("Error in retrieving results", e);
        } catch (FileTransferException e) {
            log.error("Error occurred while file transferring "+e.getLocalizedMessage());
        } catch (FileNotFoundException e) {
            log.error("Could not found stdout or stderr file "+e.getLocalizedMessage());
        } catch (IOException e) {
            log.error("Error occurred "+e.getLocalizedMessage());
        } catch (RegistryException e) {
            log.error("Error occurred while persisting data "+e.getLocalizedMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
        jCloudsUtils.stopNode();
    }

    public String stageOutputFiles(JobExecutionContext jobExecutionContext,String paramValue) throws GFacHandlerException {
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
        int i=paramValue.lastIndexOf(File.separator);
        String fileName=paramValue.substring(i+1);
        String targetFile=null;
        try{
            targetFile=app.getOutputDataDirectory()+File.separator+fileName;
            fileTransfer.downloadFilesFromEc2(paramValue,targetFile);
        }catch (FileTransferException e){
            log.error("Error while downloading file "+paramValue+" :"+e.toString());
            throw new GFacHandlerException("Error occurred while while downloading file "+e.getLocalizedMessage());
        }
        return targetFile;
    }

    public void stages3Files(JobExecutionContext jobExecutionContext,String paramValue){
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
        int i=paramValue.lastIndexOf(File.separator);
        String fileName=paramValue.substring(i+1);
        String targetFile=null;

        try{
            targetFile=app.getOutputDataDirectory()+File.separator+fileName;
            if (!jCloudsUtils.isS3CmdInstall(credentials)){
                jCloudsUtils.installS3CmdOnNode(securityContext,credentials);
            }
            String command="s3cmd put "+targetFile+" "+paramValue;
            ExecResponse response=jCloudsUtils.runScriptOnNode(credentials, command, false);
            int exitStatus=response.getExitStatus();
            if(exitStatus ==0){
              log.info("Successfully put the file "+targetFile+" to s3 ");
            }else{
              log.info("fail to put the file "+targetFile+" to s3");
            }
        }catch (Exception e){
            log.error("Error while putting file to s3");
        }

    }

    public List<String> getOutPutFileList(JobExecutionContext jobExecutionContext){
        ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
        List<String> outputFileList=new ArrayList<String>();
        String outputDirectoryName=null;
        ExecResponse execResponse=null;
        try{
            outputDirectoryName=app.getOutputDataDirectory();
            String command="ls "+outputDirectoryName;
            execResponse=jCloudsUtils.runScriptOnNode(credentials,command,false);
            String output=execResponse.getOutput();
            if(!output.equals("")){
              outputFileList= Arrays.asList(output.split("\r\n"));
            }
        }catch (Exception e){
            log.error("Error occurred while getting output file list");
        }
        return outputFileList;
    }

    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {
    }
}

