package org.apache.airavata.gfac.jclouds;

import org.apache.airavata.common.utils.AiravataUtils;
import org.apache.airavata.common.utils.DBUtil;
import org.apache.airavata.common.utils.DatabaseTestCases;
import org.apache.airavata.common.utils.DerbyUtil;
import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.credential.store.credential.Credential;
import org.apache.airavata.credential.store.credential.impl.ec2.Ec2Credential;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.credential.store.store.impl.Ec2CredentialWriter;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.core.context.ApplicationContext;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.apache.airavata.gfac.jclouds.handler.JCloudsInHandler;
import org.apache.airavata.gfac.jclouds.handler.JCloudsOutHandler;
import org.apache.airavata.gfac.jclouds.provider.impl.JCloudsProvider;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;
import org.apache.airavata.model.workspace.experiment.Experiment;
import org.apache.airavata.model.workspace.experiment.TaskDetails;
import org.apache.airavata.persistance.registry.jpa.impl.LoggingRegistryImpl;
import org.apache.airavata.schemas.gfac.*;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by udara on 7/8/14.
 */
public class JCloudsProviderTestWithStringType extends DatabaseTestCases {

    private JobExecutionContext jobExecutionContext;
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
    private String gatewayId="gatewayABC";
    private String tockenId="token123";
    private String user="user123";

    private static final String inputFile1 ="palaapayayayayayayayayayayayayahukahuakahauakahauakahauakahauakahauakahauakahauakahauakahauakajhauakahjau";
    private static final String inputFile2="atetaeaateataeataeataeataeataeataeataeapskakakapakapakapakapakapakapakapakapakapakapakapakapakapakapakapakapakaka";

    public void setUpDatabase() throws Exception {
        AiravataUtils.setExecutionAsServer();

        System.setProperty("credential.store.keystore.url", "/usr/local/AiravataNewProject/airavata/modules/configuration/server/src/main/resources/airavata.jks");
        System.setProperty("credential.store.keystore.alias", "airavata");
        System.setProperty("credential.store.keystore.password", "airavata");

/*        DerbyUtil.startDerbyInServerMode(getHostAddress(),getPort(),getUserName(),getPassword());

        waitTillServerStarts();*/

/*        String createTable = "CREATE TABLE CREDENTIALS\n" + "(\n"
                + "        GATEWAY_ID VARCHAR(256) NOT NULL,\n"
                + "        TOKEN_ID VARCHAR(256) NOT NULL,\n"
                + // Actual token used to identify the credential
                "        CREDENTIAL BLOB NOT NULL,\n" + "PORTAL_USER_ID VARCHAR(256) NOT NULL,\n"
                + "        TIME_PERSISTED TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                + "        PRIMARY KEY (GATEWAY_ID, TOKEN_ID)\n" + ")";

        String dropTable = "drop table CREDENTIALS";

        try {
            executeSQL(dropTable);
        } catch (Exception e) {
        }

        executeSQL(createTable);

        Credential credential=(Credential)(new Ec2Credential("AKIAJROJYO4DSASPFNKQ","ji9p3+10+9T+HCxVq17PE7Rt+XaleleYZCH7KgG0",null,gatewayId,userName));
        credential.setToken("token123");
        credential.setPortalUserName(user);

        add a ec2Credential for gatewayId gatewayABC tokenId token123
        Ec2CredentialWriter ec2CredentialWriter=new Ec2CredentialWriter(getDbUtil());
        ec2CredentialWriter.writeCredentials(credential);*/
    }

   @Before
   public void setup() throws Exception{
       setUpDatabase();            // set up database

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
       name.setStringValue("StringMerge");
       app.setApplicationName(name);
       app.setExecutable("/home/ec2-user/mergeString.sh");
       app.setExecutableType("sh");
       app.setJobType(JobTypeType.EC_2);

       // service
       ServiceDescription serv = new ServiceDescription();
       serv.getType().setName("fileMerge");

       //Job location
       String tempDir="/home/ec2-user";
       app.setStaticWorkingDirectory(tempDir);
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

       jobExecutionContext.setExperimentID("AiravataExperiment");
       jobExecutionContext.setExperiment(new Experiment("test123","project1","admin","testExp"));
       jobExecutionContext.setTaskData(new TaskDetails(jobExecutionContext.getExperimentID()));
       jobExecutionContext.setRegistry(new LoggingRegistryImpl());
       jobExecutionContext.setGatewayID("Gateway123");
   }

   @Test
   public void testJCloudsProvider(){
       JCloudsInHandler inHandler=new JCloudsInHandler();
       JCloudsProvider provider=new JCloudsProvider();
       JCloudsOutHandler outHandler=new JCloudsOutHandler();
       try{
           inHandler.invoke(jobExecutionContext);
           provider.initialize(jobExecutionContext);
           provider.execute(jobExecutionContext);
           outHandler.invoke(jobExecutionContext);
       }catch (Exception e){
           System.out.println(e.toString());
       }
   }

   public JCloudsSecurityContext getSecurityContext() throws Exception{
       DBUtil dbUtil=null;
       RequestData data=new RequestData();
       data.setGatewayId(gatewayId);
       data.setRequestUser(user);
       data.setTokenId(tockenId);
       CredentialReader reader=new CredentialReaderImpl(dbUtil);
       JCloudsSecurityContext securityContext=new JCloudsSecurityContext(userName,"aws-ec2",instanceId,reader,data);
       return securityContext;
   }


}
