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
package org.apache.airavata.gfac.jclouds.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Module;
import org.airavata.appcatalog.cpi.AppCatalog;
import org.airavata.appcatalog.cpi.AppCatalogException;
import org.apache.aiaravata.application.catalog.data.impl.AppCatalogFactory;
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.ClientSettings;
import org.apache.airavata.common.utils.DBUtil;
import org.apache.airavata.common.utils.ServerSettings;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.handler.GFacHandlerException;
import org.apache.airavata.gfac.jclouds.exceptions.NodeStartFailureException;
import org.apache.airavata.gfac.jclouds.exceptions.PublicKeyException;
import org.apache.airavata.gfac.jclouds.exceptions.RunScriptFailureException;
import org.apache.airavata.model.appcatalog.appdeployment.ApplicationDeploymentDescription;
import org.apache.airavata.model.appcatalog.computeresource.CloudJobSubmission;
import org.apache.airavata.model.appcatalog.computeresource.ComputeResourceDescription;
import org.apache.airavata.model.appcatalog.computeresource.JobSubmissionInterface;
import org.apache.airavata.model.appcatalog.computeresource.ProviderName;
import org.apache.airavata.model.workspace.Project;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.apache.airavata.persistance.registry.jpa.model.Gateway;
import org.apache.airavata.registry.cpi.Registry;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.apache.airavata.schemas.gfac.Ec2HostType;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_SCRIPT_COMPLETE;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

public class JCloudsUtils {
    private static final Logger log = LoggerFactory.getLogger(JCloudsUtils.class);

   public boolean terminateNode(ComputeService service,String nodeId){
       if (!checkNodeStateEqual(service,nodeId,NodeMetadata.Status.TERMINATED)){
           service.destroyNode(nodeId);
           return true;
       }
       return false;
   }

   public boolean checkNodeStateEqual(ComputeService service,String nodeId,NodeMetadata.Status status ){
       NodeMetadata node=service.getNodeMetadata(nodeId);
       if (node.getStatus()==status){
           return true;
       }
       return false;
   }

   public boolean startNode(ComputeService service,String nodeId) throws NodeStartFailureException {
       if (!checkNodeStateEqual(service,nodeId,NodeMetadata.Status.RUNNING)){
           try{
             service.resumeNode(nodeId);
             return true;
           }catch (Exception e){
             log.error("Fail to start ec2 Node "+nodeId);
             throw new NodeStartFailureException("Fail to start ec2 Node");
           }
       }
       return false;
   }

   public boolean stopNode(ComputeService service,String nodeId){
       if (checkNodeStateEqual(service,nodeId,NodeMetadata.Status.RUNNING)){
           try{
               service.suspendNode(nodeId);
               return true;
           }catch (Exception e){
               log.error("Fail to shut down node "+nodeId);
           }
       }
       return false;
   }

    public ExecResponse runScriptOnNode(ComputeService service,LoginCredentials credentials,String command,String nodeId,boolean runAsRoot) throws RunScriptFailureException {
        TemplateOptions options=TemplateOptions.Builder.overrideLoginCredentials(credentials);
        try {
            ExecResponse response=service.runScriptOnNode(nodeId,exec(command),
                    options.runAsRoot(runAsRoot).wrapInInitScript(false));
            System.out.println("response :"+ response.toString());
            return response;
        }catch (Exception e){
            log.error("fail to run command "+command);
            throw new RunScriptFailureException("fail to run command "+command);
        }
    }

    public ListenableFuture<ExecResponse> submitScriptToNode(ComputeService service,LoginCredentials credentials,String command,String nodeId,boolean runAsRoot) throws RunScriptFailureException {
        TemplateOptions options=TemplateOptions.Builder.overrideLoginCredentials(credentials);

        ListenableFuture<ExecResponse> future=null;
        try{
           future= service.submitScriptOnNode(nodeId, exec(command),
                    options.runAsRoot(runAsRoot).wrapInInitScript(false));
        }catch (Exception e){
            log.error("fail to run command "+command);
            throw new RunScriptFailureException("fail to run command "+command);
        }
        return future;
    }

    public NodeMetadata createNode(ComputeService service,String group,int number){
        NodeMetadata node=null;
        Template template=service.templateBuilder()
                .imageId("us-east-1/ami-fb8e9292")
                .os64Bit(true)
                .minRam(500)
                .smallest()
                .build();
        try{
            node=getOnlyElement(service.createNodesInGroup("default",1,template));
            System.out.print(node.getCredentials().getUser());
            System.out.print(node.getCredentials().getPrivateKey());
            System.out.print(node.getCredentials().getPassword());
            System.out.println("node is: " + node.getId() + " public address " + node.getPublicAddresses() + " private addredd " + node.getPrivateAddresses());
            return node;
        }catch (RunNodesException e){
            e.printStackTrace();
            return null;
        }
    }

    public boolean isS3CmdInstall(ComputeService service,String nodeId,LoginCredentials credentials){
        String command="s3cmd --version";
        ExecResponse response= null;
        try {
            response = runScriptOnNode(service,credentials,command,nodeId,false);
            if(response.getExitStatus()==12){
                return true;
            }
        } catch (RunScriptFailureException e) {
            return false;
        }
        return false;
    }

    public void installS3CmdOnNode(ComputeService service,JCloudsSecurityContext securityContext,LoginCredentials credentials,String nodeId) throws GFacHandlerException {
        String installs3cmd=new StringBuilder().append("cd /etc/yum.repos.d \n")
                .append("sudo wget http://s3tools.org/repo/RHEL_6/s3tools.repo \n")
                .append("sudo yum -y install s3cmd")
                .toString();

        String configures3cmd =new StringBuilder().append("s3cmd --configure \n")
                .append(securityContext.getAccessKey()+"\n")
                .append(securityContext.getSecretKey()+"\n")
                .append("\n")
                .append("\n")
                .append("\n")
                .append("\n")
                .append("Y\n")
                .append("y")
                .toString();

        try {
            runScriptOnNode(service,credentials,installs3cmd,nodeId,true);
            runScriptOnNode(service,credentials,configures3cmd,nodeId,true);
        } catch (RunScriptFailureException e) {
            log.error("Fail to install s3cmd ");
            throw new GFacHandlerException("Fail to install s3cmd ");
        }
    }

    public ComputeServiceContext initJCloudsEnvironment(JobExecutionContext jobExecutionContext) throws GFacException {
        JCloudsSecurityContext securityContext=null;
        if(jobExecutionContext!=null){
            securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        }else{
            throw new GFacException("job execution context is null");
        }

        // validate security context
        if(securityContext.getNodeId()==null || securityContext.getNodeId().isEmpty()){
            throw new GFacException("node id is empty");
        }
        if (securityContext.getProviderName()==null || securityContext.getProviderName().isEmpty()){
            throw new GFacException("cloud provider name is empty");
        }
        if(securityContext.getAccessKey()==null || securityContext.getAccessKey().isEmpty()){
            throw new GFacException("access key is empty");
        }
        if (securityContext.getSecretKey()==null || securityContext.getSecretKey().isEmpty()) {
            throw new GFacException("secret key is empty");
        }
        if(securityContext.getUserName()==null || securityContext.getUserName().isEmpty()){
            throw new GFacException("username is empty");
        }

        String nodeId=securityContext.getNodeId();

        Properties overides=new Properties();
        long scriptTimeout= TimeUnit.MILLISECONDS.convert(20,TimeUnit.MINUTES);
        overides.setProperty(TIMEOUT_SCRIPT_COMPLETE,scriptTimeout+"");

        Iterable<Module> modules= ImmutableSet.<Module>of(new Log4JLoggingModule(),
                new SshjSshClientModule());

        ComputeServiceContext context=null;
        try {
            ContextBuilder builder=ContextBuilder.newBuilder(securityContext.getProviderName())
                    .credentials(securityContext.getAccessKey(), securityContext.getSecretKey())
                    .modules(modules)
                    .overrides(overides);
            context= builder.buildView(ComputeServiceContext.class);
            startNode(context.getComputeService(),nodeId);
        } catch (NodeStartFailureException e) {
            log.error("Fail to start ec2 node");
            throw new GFacException("Fail to start ec2 node");
        }catch (Exception e){
            log.error("Fail to create ComputeService Context");
            throw new GFacException("Fail to create ComputeService Context");
        }
        securityContext.setContext(context);
        return context;
    }

    public LoginCredentials makeLoginCredentials(JCloudsSecurityContext securityContext) throws PublicKeyException, GFacException {
        String user=securityContext.getUserName();
        String privateKey=securityContext.getPublicKey();
        LoginCredentials credentials=null;

        if(privateKey==null || privateKey.equals("")){
            log.info("the public key for the node does not valid");
            throw new PublicKeyException("the public key for the node does not valid");
        }
        try{
            credentials= LoginCredentials.builder().user(user)
                    .privateKey(privateKey).build();
        }catch (Exception e){
           log.error("fail to create login credentials for the node");
           throw new GFacException("fail to create login credentials for the node");
        }
        return credentials;
    }
}

