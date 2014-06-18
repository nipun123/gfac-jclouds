package org.apache.airavata.gfac.handler;

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.MappingFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.handler.AbstractHandler;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.utils.JCloudsUtils;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.schemas.gfac.ApplicationDeploymentDescriptionType;
import org.apache.airavata.schemas.gfac.StringParameterType;
import org.apache.airavata.schemas.gfac.URIParameterType;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.domain.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.JCloudsSecurityContext;

import java.io.File;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/27/14
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class InHandler extends AbstractHandler{
    private static final Logger log = LoggerFactory.getLogger(InHandler.class);
    private JCloudsFileTransfer transfer;
    private JCloudsUtils jCloudsUtils;

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        try {
            jCloudsUtils=new JCloudsUtils();
            jCloudsUtils.initJCloudsEnvironment(jobExecutionContext);
        }catch (GFacException e){
            e.printStackTrace();
        }
        DataTransferDetails detail = new DataTransferDetails();
        TransferStatus status = new TransferStatus();
        MessageContext inputNew=new MessageContext();

        try{
           log.info("Invoking Handler");
           super.invoke(jobExecutionContext);

           MessageContext inputs=jobExecutionContext.getInMessageContext();
           Set<String> parameters=inputs.getParameters().keySet();
           for(String paramName:parameters){
               ActualParameter actualParameter=(ActualParameter)inputs.getParameters().get(paramName);
               String paramValue=MappingFactory.toString(actualParameter);

               if("URI".equals(actualParameter.getType().getType().toString())){
                   ((URIParameterType)actualParameter.getType()).setValue(stageInputFiles(jobExecutionContext,paramValue));
               }else{
                  // add the logic when S3Type add to the gfac-schema
               }
               inputNew.getParameters().put(paramName,actualParameter);
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
       int i=paramValue.lastIndexOf(File.separator);
       String fileName=paramValue.substring(i+1);
       String targetFile=null;
       try{
            targetFile=app.getInputDataDirectory()+File.separator+fileName;
            transfer.uploadFileToEc2(targetFile,paramValue);
       }catch (Exception e){
            e.printStackTrace();
       }
       return targetFile;
    }

    private String stageS3Files(JobExecutionContext jobExecutionContext,String paramValue){
       ApplicationDeploymentDescriptionType app=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription().getType();
       int i=paramValue.lastIndexOf(File.separator);
       String fileName=paramValue.substring(i+1);
       String targetFile=null;
       try {
            targetFile=app.getInputDataDirectory()+File.separator+fileName;
            if(!JCloudsUtils.isS3CmdInstall(null,"",null)){
                JCloudsUtils.installS3CmdOnNode((JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT),null,"",null);
            }
            String command="s3cmd get "+paramValue+" "+targetFile;
            JCloudsUtils.runScriptOnNode(null,"",command,null,false);
       }catch (Exception e){
          e.printStackTrace();
       }
       return targetFile;
    }

    private void makeDirectory(){

    }
    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {
    }
}
