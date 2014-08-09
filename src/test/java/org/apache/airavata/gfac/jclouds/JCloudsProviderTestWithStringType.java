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

package org.apache.airavata.gfac.jclouds;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import org.apache.airavata.api.client.AiravataClientFactory;
import org.apache.airavata.client.AiravataAPIFactory;
import org.apache.airavata.client.api.AiravataAPI;
import org.apache.airavata.client.api.exception.AiravataAPIInvocationException;
import org.apache.airavata.client.impl.PasswordCallBackImpl;
import org.apache.airavata.common.exception.AiravataConfigurationException;
import org.apache.airavata.common.utils.*;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.credential.store.credential.Credential;
import org.apache.airavata.credential.store.credential.impl.ec2.Ec2Credential;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.CredentialStoreException;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.credential.store.store.impl.Ec2CredentialWriter;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.core.context.ApplicationContext;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.cpi.BetterGfacImpl;
import org.apache.airavata.gfac.core.cpi.GFac;
import org.apache.airavata.gfac.core.cpi.GFacImpl;
import org.apache.airavata.gfac.core.monitor.state.JobStatusChangeRequest;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.jclouds.Monitoring.JCloudMonitorHandler;
import org.apache.airavata.gfac.jclouds.handler.JCloudsInHandler;
import org.apache.airavata.gfac.jclouds.handler.JCloudsOutHandler;
import org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.apache.airavata.model.error.AiravataClientConnectException;
import org.apache.airavata.model.util.ExperimentModelUtil;
import org.apache.airavata.model.workspace.experiment.*;
import org.apache.airavata.persistance.registry.jpa.impl.LoggingRegistryImpl;
import org.apache.airavata.persistance.registry.jpa.impl.RegistryFactory;
import org.apache.airavata.persistance.registry.jpa.model.WorkflowNodeDetail;
import org.apache.airavata.registry.api.AiravataRegistry2;
import org.apache.airavata.registry.api.AiravataRegistryFactory;
import org.apache.airavata.registry.api.AiravataUser;
import org.apache.airavata.registry.api.Gateway;
import org.apache.airavata.registry.api.exception.RegAccessorInstantiateException;
import org.apache.airavata.registry.api.exception.RegAccessorInvalidException;
import org.apache.airavata.registry.api.exception.RegAccessorUndefinedException;
import org.apache.airavata.registry.api.exception.RegException;
import org.apache.airavata.registry.cpi.ChildDataType;
import org.apache.airavata.registry.cpi.Registry;
import org.apache.airavata.registry.cpi.RegistryException;
import org.apache.airavata.registry.cpi.RegistryModelType;
import org.apache.airavata.schemas.gfac.*;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.airavata.api.Airavata;
import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.*;

public class JCloudsProviderTestWithStringType{

    private DBUtil dbUtil;
    private JobExecutionContext jobExecutionContext;
    private Registry registry;

    private static final String DEFAULT_USER = "default.registry.user";
    private static final String DEFAULT_USER_PASSWORD = "default.registry.password";
    private static final String DEFAULT_GATEWAY = "default.registry.gateway";

    /* Username used to log into your ec2 instance eg.ec2-user */
    private String userName = "ec2-user";

    /* Secret key used to connect to the image */
    private String secretKey = "";

    /* Access key used to connect to the image */
    private String accessKey = "";

    /* Instance id of the running instance of your image */
    private String instanceId = "i-3040241c";

    private String hostName="ec2";
    private String hostAddress="";
    private String gatewayId="php_reference_gateway";
    private String tockenId;
    private String user="user123";
    final String experimentId="StringMergeExperiment_477c1d67-287d-4c99-9f87-5e0313e1e452";

    private static final String inputFile1 ="yarayarayargahataraobamaghebnammaobenamapiyamukoheharikahauakahauakahauakahauakahauakahauakajhauakahjau";
    private static final String inputFile2="oyanisahadalamageasuthridunatarahawelaharagiyadapaluweunnewedanadanune";

    public void setUpDatabase() throws Exception {
        System.setProperty("credential.store.keystore.url", "/usr/local/AiravataNewProject/airavata/modules/configuration/server/src/main/resources/airavata.jks");
        System.setProperty("credential.store.keystore.alias", "airavata");
        System.setProperty("credential.store.keystore.password", "airavata");
        System.setProperty("credential.store.jdbc.url","jdbc:derby://localhost:1527/credential_store;create=true;user=admin;password=admin");
        System.setProperty("credential.store.jdbc.user","admin");
        System.setProperty("credential.store.jdbc.password","admin");
        System.setProperty("credential.store.jdbc.driver","org.apache.derby.jdbc.ClientDriver");
        System.setProperty("activity.listeners","org.apache.airavata.gfac.core.monitor.AiravataJobStatusUpdator,org.apache.airavata.gfac.core.monitor.AiravataTaskStatusUpdator," +
                "org.apache.airavata.gfac.core.monitor.AiravataWorkflowNodeStatusUpdator,org.apache.airavata.gfac.core." +
                "monitor.GfacInternalStatusUpdator");
        System.setProperty("jpa.cache.size","500");

        dbUtil=new DBUtil("jdbc:derby://localhost:1527/credential_store;create=true;user=admin;password=admin",
                "admin", "admin", "org.apache.derby.jdbc.ClientDriver");


        String createTable = "CREATE TABLE CREDENTIALS\n" + "(\n"
                + "        GATEWAY_ID VARCHAR(256) NOT NULL,\n"
                + "        TOKEN_ID VARCHAR(256) NOT NULL,\n"
                + // Actual token used to identify the credential
                "        CREDENTIAL BLOB NOT NULL,\n" + "PORTAL_USER_ID VARCHAR(256) NOT NULL,\n"
                + "        TIME_PERSISTED TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                + "        PRIMARY KEY (GATEWAY_ID, TOKEN_ID)\n" + ")";

        String dropTable = "drop table CREDENTIALS";

        try {
            dbUtil.executeSQL(dropTable);
        } catch (Exception e) {
        }

        dbUtil.executeSQL(createTable);

        Ec2Credential credential=new Ec2Credential(accessKey,secretKey,null,gatewayId,userName);
        UUID uuid = UUID.randomUUID();
        tockenId=uuid.toString();
        System.out.println("TokenId: " + tockenId);
        credential.setToken(uuid.toString());
        credential.setPortalUserName("user123");

        FileInputStream fis= new FileInputStream("/etc/ssh/.ssh/airavata.pem");
        byte[] bytes=new byte[(int)fis.getChannel().size()];
        fis.read(bytes);
        credential.setPublickey(bytes);
        Ec2CredentialWriter writer=new Ec2CredentialWriter(dbUtil);
        try {
            writer.writeCredentials(credential);
        } catch (CredentialStoreException e) {
            e.printStackTrace();
        }
    }

   @Before
   public void setup() throws Exception{
       setUpDatabase();            // set up database

       URL resource = JCloudsProviderTestWithStringType.class.getClassLoader().getResource(org.apache.airavata.common.utils.Constants.GFAC_CONFIG_XML);
       assert resource != null;
       GFacConfiguration gFacConfiguration = GFacConfiguration.create(new File(resource.getPath()), null, null);
       gFacConfiguration.setOutHandlers("org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider",null);
       gFacConfiguration.setInHandlers("org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider",null);

       // host
       HostDescription host = new HostDescription(Ec2HostType.type);
       host.getType().setHostName(instanceId);
       host.getType().setHostAddress(hostAddress);

       //Node
       WorkflowNodeDetails nodeDetail=new WorkflowNodeDetails();
       nodeDetail.setNodeInstanceId("IDontNeedaNode_063ba363-d840-4b4b-901a-6ef366e8d4d1");

       // app
       ApplicationDescription ec2Desc = new ApplicationDescription(Ec2ApplicationDeploymentType.type);
       Ec2ApplicationDeploymentType app = (Ec2ApplicationDeploymentType)ec2Desc.getType();
       ApplicationDeploymentDescriptionType.ApplicationName name = ApplicationDeploymentDescriptionType.ApplicationName.Factory.newInstance();
       name.setStringValue("StringMerge");
       app.setApplicationName(name);
       app.setExecutable("/home/ec2-user/mergeString.sh");
       app.setExecutableType("sh");

       // service
       ServiceDescription serv = new ServiceDescription();
       serv.getType().setName("StringMerge");

       //Job location
       String tempDir="/home/ec2-user";
       app.setScratchWorkingDirectory(tempDir);
       app.setInputDataDirectory(tempDir+"/input");
       app.setOutputDataDirectory(tempDir +"/output");
       app.setStandardOutput(tempDir +"/stdout");
       app.setStandardError(tempDir +"/stderr");


       //Inputs and Outputs
       List<InputParameterType> inputList = new ArrayList<InputParameterType>();

       InputParameterType input1=InputParameterType.Factory.newInstance();
       input1.setParameterName("input1");
       input1.setParameterType(StringParameterType.Factory.newInstance());
       inputList.add(input1);

       InputParameterType input2=InputParameterType.Factory.newInstance();
       input2.setParameterName("input2");
       input2.setParameterType(StringParameterType.Factory.newInstance());
       inputList.add(input2);

       InputParameterType[] inputParameters=inputList.toArray(new InputParameterType[inputList.size()]);

       List<OutputParameterType> outputList = new ArrayList<OutputParameterType>();

       OutputParameterType output1=OutputParameterType.Factory.newInstance();
       output1.setParameterName("output");
       output1.setParameterType(StringParameterType.Factory.newInstance());
       outputList.add(output1);

       OutputParameterType[] outputParameters=outputList.toArray(new OutputParameterType[outputList.size()]);

       serv.getType().setOutputParametersArray(outputParameters);
       serv.getType().setInputParametersArray(inputParameters);

       jobExecutionContext=new JobExecutionContext(gFacConfiguration,serv.getType().getName());
       ApplicationContext applicationContext=new ApplicationContext();
       jobExecutionContext.setWorkflowNodeDetails(nodeDetail);
       jobExecutionContext.setApplicationContext(applicationContext);
       applicationContext.setServiceDescription(serv);
       applicationContext.setApplicationDeploymentDescription(ec2Desc);
       applicationContext.setHostDescription(host);

       JCloudsSecurityContext securityContext=getSecurityContext();
       jobExecutionContext.addSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT,securityContext);

       MessageContext inMessage=new MessageContext();
       ActualParameter inputParam1=new ActualParameter();
       ((StringParameterType)inputParam1.getType()).setValue(inputFile1);
       inMessage.addParameter("input1",inputParam1);
       System.out.println(inputParam1.getType().getType().toString());

       ActualParameter inputParam2=new ActualParameter();
       ((StringParameterType)inputParam2.getType()).setValue(inputFile2);
       inMessage.addParameter("input2",inputParam2);

       MessageContext outMessage=new MessageContext();
       ActualParameter outputParam=new ActualParameter();
       outMessage.addParameter("output",outputParam);

       jobExecutionContext.setInMessageContext(inMessage);
       jobExecutionContext.setOutMessageContext(outMessage);

       jobExecutionContext.setExperimentID(experimentId);
       jobExecutionContext.setExperiment(new Experiment(experimentId, "project1_b3f3548c-b7ad-489b-a455-dbd4b5bba78f", "admin", "StringMergeExperiment"));
       jobExecutionContext.setTaskData(new TaskDetails("IDontNeedaNode_b338a28c-09d0-43f8-8559-255a95ec6310"));
       jobExecutionContext.setGatewayID("php_reference_gateway");
       jobExecutionContext.setCredentialStoreToken(tockenId);
   }

   @Test
   public void testJCloudsProvider() throws RegistryException {
       GFacImpl gFac= null;
       try {
           String sysUser = ClientSettings.getSetting(DEFAULT_USER);
           String sysUserPwd = ClientSettings.getSetting(DEFAULT_USER_PASSWORD);
           String gateway = ClientSettings.getSetting(DEFAULT_GATEWAY);
           registry = RegistryFactory.getRegistry(gateway, sysUser, sysUserPwd);

           gFac=new GFacImpl(registry, null,
              AiravataRegistryFactory.getRegistry(new Gateway("default"),
                           new AiravataUser("admin")));
           jobExecutionContext.setRegistry(registry);
           jobExecutionContext.setGfac(gFac);

           /*MonitorPublisher publisher = new MonitorPublisher(new EventBus());
           BetterGfacImpl.setMonitorPublisher(publisher);

           gFac = new BetterGfacImpl(registry, null,
                   AiravataRegistryFactory.getRegistry(new Gateway(gateway),
                           new AiravataUser(sysUser)),null,publisher);*/

           //gFac.submitJob(experimentId,"IDontNeedaNode_5af93bc4-5ebd-40ba-8ec1-f5f96c05bae5","php_reference_gateway");
           monitor();
           gFac.submitJob(jobExecutionContext);
           while(true){

           }
       } catch (Exception e) {
           e.printStackTrace();
       }
   }

   public JCloudsSecurityContext getSecurityContext() throws Exception{
       RequestData data=new RequestData();
       data.setGatewayId(gatewayId);
       data.setRequestUser(user);
       data.setTokenId(tockenId);
       CredentialReader reader=new CredentialReaderImpl(dbUtil);
       JCloudsSecurityContext securityContext=new JCloudsSecurityContext(userName,"aws-ec2",instanceId,reader,data);
       return securityContext;
   }

   public void createTaskAndLaunch() throws RegistryException {
       Experiment experiment = (Experiment) registry.get(RegistryModelType.EXPERIMENT, experimentId);
       WorkflowNodeDetails iDontNeedaNode = ExperimentModelUtil.createWorkflowNode("IDontNeedaNode", null);
       String nodeID = (String) registry.add(ChildDataType.WORKFLOW_NODE_DETAIL, iDontNeedaNode, experimentId);
       TaskDetails taskDetails = ExperimentModelUtil.cloneTaskFromExperiment(experiment);
       taskDetails.setTaskID((String) registry.add(ChildDataType.TASK_DETAIL, taskDetails, nodeID));
   }

   public void monitor() throws AiravataClientConnectException {
       final Airavata.Client airavata = AiravataClientFactory.createAiravataClient("localhost", 8930);
       Thread monitor = (new Thread(){
           public void run() {
               Map<String, JobStatus> jobStatuses = null;
               while (true) {
                   try {
                       jobStatuses = airavata.getJobStatuses(experimentId);
                       Set<String> strings = jobStatuses.keySet();
                       for (String key : strings) {
                           JobStatus jobStatus = jobStatuses.get(key);
                           if(jobStatus == null){
                               return;
                           }else {
                               if (JobState.COMPLETE.equals(jobStatus.getJobState())) {
                                   System.out.println("Job completed Job ID: " + jobStatus.getJobState().toString());
                                   return;
                               }else{
                                   System.out.println("Job ID:" + key + jobStatuses.get(key).getJobState().toString());
                               }
                           }
                       }
                       Thread.sleep(20000);
                   } catch (Exception e) {
                       e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                   }
               }
           }
       });
       monitor.start();
   }


}

