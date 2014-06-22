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

import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.core.context.ApplicationContext;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.jclouds.handler.JCloudsInHandler;
import org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.apache.airavata.model.workspace.experiment.WorkflowNodeDetails;
import org.apache.airavata.persistance.registry.jpa.impl.LoggingRegistryImpl;
import org.apache.airavata.schemas.gfac.*;
import org.junit.Before;
import org.junit.Test;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class JCloudsProviderTest {
    private JobExecutionContext jobExecutionContext;
    /* Username used to log into your ec2 instance eg.ec2-user */
    private String userName = "ec2-user";

    /* Secret key used to connect to the image */
    private String secretKey = "10VE/FvtTuXtehmw/+buEzk3nrS+Kc4uiX+setW+";

    /* Access key used to connect to the image */
    private String accessKey = "AKIAJ3M3FUZ7PTDQP4YQ";

    /* Instance id of the running instance of your image */
    private String instanceId = "i-b9803a92";

    private String hostName="ec2";
    private String hostAddress="";

    private static final String inputFile1="C:/Project/AiravataProject/Job/input1.txt";
    private static final String inputFile2="C:/Project/AiravataProject/Job/input2.txt";
    private static final String outputFile="C:/Project/AiravataProject/Job/output.txt";

    @Before
    public void setup() throws Exception{
        URL resource = InHandlerTest.class.getClassLoader().getResource(org.apache.airavata.common.utils.Constants.GFAC_CONFIG_XML);
        assert resource != null;
        System.out.println(resource.getFile());
        GFacConfiguration gFacConfiguration = GFacConfiguration.create(new File(resource.getPath()), null, null);

        // host
        HostDescription host = new HostDescription(Ec2HostType.type);
        host.getType().setHostName(hostName);
        host.getType().setHostAddress(hostAddress);

        // app
        ApplicationDescription ec2Desc = new ApplicationDescription(Ec2ApplicationDeploymentType.type);
        Ec2ApplicationDeploymentType app = (Ec2ApplicationDeploymentType)ec2Desc.getType();
        ApplicationDeploymentDescriptionType.ApplicationName name = ApplicationDeploymentDescriptionType.ApplicationName.Factory.newInstance();
        name.setStringValue("fileMerge");
        app.setApplicationName(name);
        app.setExecutable("/home/ec2-user/merge.sh");
        app.setExecutableType("sh");
        app.setJobType(JobTypeType.EC_2);

        // service
        ServiceDescription serv = new ServiceDescription();
        serv.getType().setName("fileMerge");

        //Job location
        String tempDir="/home/ec2-user";
        app.setInputDataDirectory(tempDir+"/input");
        app.setOutputDataDirectory(tempDir +"/output");
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

        JCloudsSecurityContext securityContext=new JCloudsSecurityContext(userName,"aws-ec2",accessKey,secretKey,instanceId);
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
        ((URIParameterType)outputParam.getType()).setValue(outputFile);
        outMessage.addParameter("output",outputParam);

        jobExecutionContext.setInMessageContext(inMessage);
        jobExecutionContext.setOutMessageContext(outMessage);

        jobExecutionContext.setExperimentID("AiravataExperiment");
        jobExecutionContext.setExperiment(new Experiment("test123","project1","admin","testExp"));
        jobExecutionContext.setTaskData(new TaskDetails(jobExecutionContext.getExperimentID()));
        jobExecutionContext.setRegistry(new LoggingRegistryImpl());
        jobExecutionContext.setWorkflowNodeDetails(new WorkflowNodeDetails(jobExecutionContext.getExperimentID(),"none"));
    }

    @Test
    public void testInitialize(){
        JCloudsInHandler inHandler=new JCloudsInHandler();
        JCloudsProvider provider=new JCloudsProvider();
        try{
            inHandler.invoke(jobExecutionContext);
            provider.initialize(jobExecutionContext);
            provider.execute(jobExecutionContext);
        }catch (Exception e){
           System.out.println(e.toString());
        }
    }
}
