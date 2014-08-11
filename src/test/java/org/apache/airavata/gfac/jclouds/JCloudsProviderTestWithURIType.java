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

import org.apache.airavata.common.utils.DBUtil;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.credential.store.credential.impl.ec2.Ec2Credential;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.CredentialStoreException;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.credential.store.store.impl.Ec2CredentialWriter;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.core.context.ApplicationContext;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.cpi.GFacImpl;
import org.apache.airavata.gfac.jclouds.handler.JCloudsInHandler;
import org.apache.airavata.gfac.jclouds.handler.JCloudsOutHandler;
import org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.apache.airavata.model.workspace.experiment.WorkflowNodeDetails;
import org.apache.airavata.persistance.registry.jpa.impl.LoggingRegistryImpl;
import org.apache.airavata.persistance.registry.jpa.impl.RegistryFactory;
import org.apache.airavata.registry.cpi.Registry;
import org.apache.airavata.schemas.gfac.*;
import org.junit.Before;
import org.junit.Test;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class JCloudsProviderTestWithURIType {
    private JobExecutionContext jobExecutionContext;

    /* Username used to log into your ec2 instance eg.ec2-user */
    private String userName = "ec2-user";

    /* Instance id of the running instance of your image */
    private String instanceId = "i-3040241c";

    private String user="user123";
    private String tockenId="5dcba6fe-8e0b-4df6-bfc1-5f7dc5edd8fc";
    private String gatewayId="php_reference_gateway";

    private static final String inputFile1="/usr/local/project/files/input1.txt";
    private static final String inputFile2="/usr/local/project/files/input2.txt";

    @Before
    public void setup() throws Exception{
        System.setProperty("credential.store.keystore.url", "/usr/local/project/airavata/modules/configuration/server/src/main/resources/airavata.jks");
        System.setProperty("credential.store.keystore.alias", "airavata");
        System.setProperty("credential.store.keystore.password", "airavata");
        System.setProperty("credential.store.jdbc.url","jdbc:derby://localhost:1527/credential_store;create=true;user=admin;password=admin");
        System.setProperty("credential.store.jdbc.user","admin");
        System.setProperty("credential.store.jdbc.password","admin");
        System.setProperty("credential.store.jdbc.driver","org.apache.derby.jdbc.ClientDriver");

        URL resource = InHandlerTest.class.getClassLoader().getResource(org.apache.airavata.common.utils.Constants.GFAC_CONFIG_XML);
        assert resource != null;
        System.out.println(resource.getFile());
        GFacConfiguration gFacConfiguration = GFacConfiguration.create(new File(resource.getPath()), null, null);
        gFacConfiguration.setOutHandlers("org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider",null);
        gFacConfiguration.setInHandlers("org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider",null);

        // host
        HostDescription host = new HostDescription(Ec2HostType.type);

        // app
        ApplicationDescription ec2Desc = new ApplicationDescription(Ec2ApplicationDeploymentType.type);
        Ec2ApplicationDeploymentType app = (Ec2ApplicationDeploymentType)ec2Desc.getType();
        ApplicationDeploymentDescriptionType.ApplicationName name = ApplicationDeploymentDescriptionType.ApplicationName.Factory.newInstance();
        name.setStringValue("fileMerge");
        app.setApplicationName(name);
        app.setExecutableLocation("/home/ec2-user/fileMerge.sh");
        app.setExecutableType("sh");
        app.setJobType(JobTypeType.EC_2);

        // service
        ServiceDescription serv = new ServiceDescription();
        serv.getType().setName("fileMerge");

        WorkflowNodeDetails workflowNodeDetails=new WorkflowNodeDetails();

        //Job location
        String tempDir="/home/ec2-user";
        app.setScratchWorkingDirectory(tempDir);
        app.setInputDataDirectory(tempDir+"/inputData");
        app.setOutputDataDirectory(tempDir +"/outputData");
        app.setStandardOutput(tempDir +"/stdout");
        app.setStandardError(tempDir +"/stderr");


        //Inputs and Outputs
        List<InputParameterType> inputList = new ArrayList<InputParameterType>();

        InputParameterType input1=InputParameterType.Factory.newInstance();
        input1.setParameterName("input1");
        input1.setParameterType(URIParameterType.Factory.newInstance());
        inputList.add(input1);

        InputParameterType input2=InputParameterType.Factory.newInstance();
        input2.setParameterName("input2");
        input2.setParameterType(URIParameterType.Factory.newInstance());
        inputList.add(input2);

        InputParameterType[] inputParameters=inputList.toArray(new InputParameterType[inputList.size()]);

        List<OutputParameterType> outputList = new ArrayList<OutputParameterType>();

        OutputParameterType output1=OutputParameterType.Factory.newInstance();
        output1.setParameterName("output");
        output1.setParameterType(URIParameterType.Factory.newInstance());
        outputList.add(output1);

        OutputParameterType[] outputParameters=outputList.toArray(new OutputParameterType[outputList.size()]);

        serv.getType().setOutputParametersArray(outputParameters);
        serv.getType().setInputParametersArray(inputParameters);

        jobExecutionContext=new JobExecutionContext(gFacConfiguration,serv.getType().getName());
        ApplicationContext applicationContext=new ApplicationContext();
        jobExecutionContext.setApplicationContext(applicationContext);
        applicationContext.setServiceDescription(serv);
        applicationContext.setApplicationDeploymentDescription(ec2Desc);
        applicationContext.setHostDescription(host);

        JCloudsSecurityContext securityContext=getSecurityContext();
        jobExecutionContext.addSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT,securityContext);

        MessageContext inMessage=new MessageContext();
        ActualParameter inputParam1=new ActualParameter();
        inputParam1.getType().changeType(URIParameterType.type);
        ((URIParameterType)inputParam1.getType()).setValue(inputFile1);
        inMessage.addParameter("input1",inputParam1);
        System.out.println(inputParam1.getType().getType().toString());

        ActualParameter inputParam2=new ActualParameter();
        inputParam2.getType().changeType(URIParameterType.type);
        ((URIParameterType)inputParam2.getType()).setValue(inputFile2);
        inMessage.addParameter("input2",inputParam2);

        MessageContext outMessage=new MessageContext();
        ActualParameter outputParam=new ActualParameter();
        outputParam.getType().changeType(URIParameterType.type);
        ((URIParameterType)outputParam.getType()).setValue("");
        outMessage.addParameter("output",outputParam);

        jobExecutionContext.setInMessageContext(inMessage);
        jobExecutionContext.setOutMessageContext(outMessage);
        jobExecutionContext.setWorkflowNodeDetails(workflowNodeDetails);
        jobExecutionContext.setExperimentID("AiravataExperiment");
        jobExecutionContext.setExperiment(new Experiment("test123","project1","admin","testExp"));
        jobExecutionContext.setTaskData(new TaskDetails(jobExecutionContext.getExperimentID()));
        jobExecutionContext.setRegistry(new LoggingRegistryImpl());
        jobExecutionContext.setRegistry(RegistryFactory.getLoggingRegistry());
        jobExecutionContext.setGatewayID("php_reference_gateway");

    }

    @Test
    public void testInitialize(){
        GFacImpl gFac= new GFacImpl(null,null,null);
        try {
            gFac.submitJob(jobExecutionContext);
        } catch (GFacException e) {
            e.printStackTrace();
        }
    }

    public JCloudsSecurityContext getSecurityContext() throws Exception{
        RequestData data=new RequestData();
        data.setGatewayId(gatewayId);
        data.setRequestUser(user);
        data.setTokenId(tockenId);
        CredentialReader reader=new CredentialReaderImpl(DBUtil.getCredentialStoreDBUtil());
        JCloudsSecurityContext securityContext=new JCloudsSecurityContext(userName,"aws-ec2",instanceId,reader,data);
        return securityContext;
    }
}

