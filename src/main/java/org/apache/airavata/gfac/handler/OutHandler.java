package org.apache.airavata.gfac.handler;

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.commons.gfac.type.ActualParameter;
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
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.apache.airavata.schemas.gfac.Ec2HostType;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.domain.Location;
import org.jclouds.io.Payload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.JCloudsSecurityContext;

import java.io.ByteArrayOutputStream;
import java.util.Properties;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/27/14
 * Time: 10:48 AM
 * To change this template use File | Settings | File Templates.
 */
public class OutHandler extends AbstractHandler {
    private static final Logger log = LoggerFactory.getLogger(OutHandler.class);
    private JCloudsUtils jCloudsUtils;
    private JCloudsFileTransfer fileTransfer;

    public void invoke(JobExecutionContext jobExecutionContext) throws GFacHandlerException{
        jCloudsUtils=new JCloudsUtils();
        fileTransfer=new JCloudsFileTransfer(jCloudsUtils.getContext(),jCloudsUtils.getSecurityContext().getNodeId(),jCloudsUtils.getCredentials());

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

    public void stages3Files(){

    }

    @Override
    public void initProperties(Properties properties) throws GFacHandlerException {
    }
}
