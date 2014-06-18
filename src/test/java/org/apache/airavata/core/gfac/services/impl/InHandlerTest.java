package org.apache.airavata.core.gfac.services.impl;

import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.core.context.ApplicationContext;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.handler.InHandler;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.apache.airavata.model.workspace.experiment.WorkflowNodeDetails;
import org.apache.airavata.persistance.registry.jpa.impl.LoggingRegistryImpl;
import org.apache.airavata.schemas.gfac.Ec2ApplicationDeploymentType;
import org.apache.airavata.schemas.gfac.Ec2HostType;
import org.apache.airavata.schemas.gfac.InputParameterType;
import org.apache.airavata.schemas.gfac.StringParameterType;
import org.apache.airavata.workflow.model.utils.MessageConstants;
import org.junit.Before;
import org.junit.Test;
import org.apache.airavata.gfac.security.JCloudsSecurityContext;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/29/14
 * Time: 6:50 PM
 * To change this template use File | Settings | File Templates.
 */
public class InHandlerTest {
    private JobExecutionContext jobExecutionContext;
    /* Username used to log into your ec2 instance eg.ec2-user */
    private String userName = "Udara";

    /* Secret key used to connect to the image */
    private String secretKey = "10VE/FvtTuXtehmw/+buEzk3nrS+Kc4uiX+setW+";

    /* Access key used to connect to the image */
    private String accessKey = "AKIAJ3M3FUZ7PTDQP4YQ";

    /* Instance id of the running instance of your image */
    private String instanceId = "i-2ec6337c";

    private String hostName="";
    private String hostAddress="";

    private static final String inputMessage1="hfjhdGBgYBFhzBXJBSSSSSSSSHCBWUTCC>jbfhytggfv";
    private static final String inputMessage2="gsVULEYTGZYUCGBakdbkhjvbhieghluir8eOBOVBi9eru";

    @Before
    public void setup() throws Exception{
        URL resource = InHandlerTest.class.getClassLoader().getResource(org.apache.airavata.common.utils.Constants.GFAC_CONFIG_XML);
        assert resource != null;
        System.out.println(resource.getFile());
        GFacConfiguration gFacConfiguration = GFacConfiguration.create(new File(resource.getPath()), null, null);

        HostDescription host = new HostDescription(Ec2HostType.type);
        host.getType().setHostName(hostName);
        host.getType().setHostAddress(hostAddress);

        ApplicationDescription ec2Desc = new ApplicationDescription(Ec2ApplicationDeploymentType.type);
        Ec2ApplicationDeploymentType ec2App = (Ec2ApplicationDeploymentType)ec2Desc.getType();

        ServiceDescription serv = new ServiceDescription();
        serv.getType().setName("EC2Test");

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

        serv.getType().setInputParametersArray(inputParameters);
        jobExecutionContext=new JobExecutionContext(gFacConfiguration,serv.getType().getName());
        ApplicationContext applicationContext=new ApplicationContext();
        jobExecutionContext.setApplicationContext(applicationContext);
        applicationContext.setServiceDescription(serv);
        applicationContext.setApplicationDeploymentDescription(ec2Desc);
        applicationContext.setHostDescription(host);

        JCloudsSecurityContext jCloudsSecurityContext=new JCloudsSecurityContext("","","","","");
        jobExecutionContext.addSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT,jCloudsSecurityContext);

        MessageContext inMessage=new MessageContext();
        ActualParameter inputParam1=new ActualParameter();
        ((StringParameterType)inputParam1.getType()).setValue(inputMessage1);
        inMessage.addParameter("input1",inputParam1);
        System.out.println(inputParam1.getType().getType().toString());

        ActualParameter inputParam2=new ActualParameter();
        ((StringParameterType)inputParam2.getType()).setValue(inputMessage2);
        inMessage.addParameter("input2",inputParam1);

        jobExecutionContext.setInMessageContext(inMessage);

        jobExecutionContext.setExperimentID("AiravataExperiment");
        jobExecutionContext.setExperiment(new Experiment("test123","project1","admin","testExp"));
        jobExecutionContext.setTaskData(new TaskDetails(jobExecutionContext.getExperimentID()));
        jobExecutionContext.setRegistry(new LoggingRegistryImpl());
        jobExecutionContext.setWorkflowNodeDetails(new WorkflowNodeDetails(jobExecutionContext.getExperimentID(),"none"));

    }

    @Test
    public void testInvoke(){
        InHandler inHandler=new InHandler();

        try{
        inHandler.invoke(jobExecutionContext);
        }catch (Exception e){
           System.out.println(e.toString());
        }
    }
}