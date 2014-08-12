package org.apache.airavata.gfac.jclouds.utils;

import org.airavata.appcatalog.cpi.AppCatalog;
import org.airavata.appcatalog.cpi.AppCatalogException;
import org.apache.aiaravata.application.catalog.data.impl.AppCatalogFactory;
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.DBUtil;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.apache.airavata.model.appcatalog.appdeployment.ApplicationDeploymentDescription;
import org.apache.airavata.model.appcatalog.computeresource.CloudJobSubmission;
import org.apache.airavata.model.appcatalog.computeresource.ComputeResourceDescription;
import org.apache.airavata.model.appcatalog.computeresource.JobSubmissionInterface;
import org.apache.airavata.model.appcatalog.computeresource.ProviderName;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by root on 8/12/14.
 */
public class SecurityUtils {
    private static final Logger log = LoggerFactory.getLogger(SecurityUtils.class);

    public static synchronized JCloudsSecurityContext addSecurityContext(JobExecutionContext jobExecutionContext) throws AppCatalogException {
        AppCatalog appCatalog = AppCatalogFactory.getAppCatalog();

        TaskDetails taskData=jobExecutionContext.getTaskData();
        String applicationDeploymentId = taskData.getApplicationDeploymentId();
        ApplicationDeploymentDescription applicationDeployment = appCatalog.
                getApplicationDeployment().getApplicationDeployement(applicationDeploymentId);
        ComputeResourceDescription computeResource = appCatalog.getComputeResource().
                getComputeResource(applicationDeployment.getComputeHostId());

        JobSubmissionInterface jobSubmissionInterface=computeResource.getJobSubmissionInterfaces().get(0);
        CloudJobSubmission cloudJobSubmission=appCatalog.getComputeResource().getCloudJobSubmission(jobSubmissionInterface.getJobSubmissionInterfaceId());
        String credentialStoreToken = jobExecutionContext.getCredentialStoreToken();                         // this is set by the framework
        RequestData requestData = null;
        String providerName=null;

        if(cloudJobSubmission.getProviderName()== ProviderName.EC2){
            providerName="aws-ec2";
        }

        JCloudsSecurityContext securityContext=null;
        try {
            requestData = new RequestData(ServerSettings.getDefaultUserGateway());
            requestData.setTokenId(credentialStoreToken);
            CredentialReader reader=new CredentialReaderImpl(DBUtil.getCredentialStoreDBUtil());
            securityContext=new JCloudsSecurityContext(cloudJobSubmission.getUserAccountName(),providerName,cloudJobSubmission.getNodeId(),reader,requestData);

        } catch (ApplicationSettingsException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        jobExecutionContext.addSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT,securityContext);
        return securityContext;
    }

}
