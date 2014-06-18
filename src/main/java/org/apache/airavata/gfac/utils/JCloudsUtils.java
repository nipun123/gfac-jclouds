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
package org.apache.airavata.gfac.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Module;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
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
import org.apache.airavata.gfac.security.JCloudsSecurityContext;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_SCRIPT_COMPLETE;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

public class JCloudsUtils {
    private String jobID = null;
    private String taskID = null;
    private JCloudsSecurityContext securityContext;
    private String nodeId;
    private ComputeServiceContext context;
    private ComputeService service;
    private LoginCredentials credentials;
    private static JCloudsUtils jCloudsUtils;

   public static JCloudsUtils getInstance(){
      if (jCloudsUtils==null){
          jCloudsUtils=new JCloudsUtils();
      }
      return jCloudsUtils;
   }

   public boolean terminateNode(){
       service.destroyNode(nodeId);
       if (checkNodeStateEqual(NodeMetadata.Status.TERMINATED)){
           return true;
       }
       return false;
   }

   public boolean checkNodeStateEqual(NodeMetadata.Status status ){
       NodeMetadata node=service.getNodeMetadata(nodeId);
       if (node.getStatus()==status){
           return true;
       }
       return false;
   }

   public boolean startNode(){
       service.resumeNode(nodeId);
       if (checkNodeStateEqual(NodeMetadata.Status.RUNNING)){
           return true;
       }
       return false;
   }

   public boolean stopNode(){
       service.suspendNode(nodeId);
       if (checkNodeStateEqual(NodeMetadata.Status.SUSPENDED)){
           return true;
       }
       return false;
   }

    public ExecResponse runScriptOnNode(LoginCredentials credentials,String command,boolean runAsRoot){
        TemplateOptions options=TemplateOptions.Builder.overrideLoginCredentials(credentials);
        try {
            ExecResponse response=service.runScriptOnNode(nodeId,exec(command),
                    options.runAsRoot(runAsRoot).wrapInInitScript(false));

            System.out.println("response :"+ response.toString());
            return response;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
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

    public boolean isS3CmdInstall(LoginCredentials credentials){
        String command="s3cmd --version";
        ExecResponse response=runScriptOnNode(credentials,command,false);
        if(response.getExitStatus()==12){
            return true;
        }
        return false;
    }

    public void installS3CmdOnNode(JCloudsSecurityContext securityContext,LoginCredentials credentials){
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

        runScriptOnNode(credentials,installs3cmd,true);
        runScriptOnNode(credentials,configures3cmd,true);
    }

    public void initJCloudsEnvironment(JobExecutionContext jobExecutionContext) throws GFacException {
        if(jobExecutionContext!=null){
            securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        }
        nodeId=securityContext.getNodeId();

        Properties overides=new Properties();
        long scriptTimeout= TimeUnit.MILLISECONDS.convert(20,TimeUnit.MINUTES);
        overides.setProperty(TIMEOUT_SCRIPT_COMPLETE,scriptTimeout+"");

        Iterable<Module> modules= ImmutableSet.<Module>of(new Log4JLoggingModule(),
                new SshjSshClientModule());
        ContextBuilder builder=ContextBuilder.newBuilder(securityContext.getProviderName())
                .credentials(securityContext.getAccessKey(), securityContext.getSecretKey())
                .modules(modules)
                .overrides(overides);
        context= builder.buildView(ComputeServiceContext.class);
        service=context.getComputeService();

        try{
            String user=System.getProperty("user.name");
            String privateKey= Files.toString(new File("C:/Users/" + System.getProperty("user.name")+ "/.ssh/airavata.pem"), UTF_8);
            credentials= LoginCredentials.builder().user(securityContext.getUserName())
                    .privateKey(privateKey).build();
        }catch (Exception e){
            System.out.println("error reading ssh key: "+e.toString());
        }

        NodeMetadata node=context.getComputeService().getNodeMetadata(nodeId);
        if(node.getStatus()== NodeMetadata.Status.SUSPENDED){
            startNode();
        }
    }

    public LoginCredentials getCredentials() {
        return credentials;
    }

    public ComputeServiceContext getContext() {
        return context;
    }

    public ComputeService getService() {
        return context.getComputeService();
    }

    public JCloudsSecurityContext getSecurityContext() {
        return securityContext;
    }
}