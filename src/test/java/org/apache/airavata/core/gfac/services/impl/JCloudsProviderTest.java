package org.apache.airavata.core.gfac.services.impl;

import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.HostDescription;
import org.apache.airavata.commons.gfac.type.ServiceDescription;
import org.apache.airavata.gfac.GFacConfiguration;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.provider.impl.JCloudsProvider;
import org.apache.airavata.schemas.gfac.Ec2ApplicationDeploymentType;
import org.apache.airavata.schemas.gfac.Ec2HostType;
import org.junit.Before;
import org.junit.Test;
import security.JCloudsSecurityContext;

import java.io.File;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/30/14
 * Time: 1:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class JCloudsProviderTest {
    private JobExecutionContext jobExecutionContext;

    @Before
    public void setup() throws Exception{
        URL resource = JCloudsProviderTest.class.getClassLoader().getResource(org.apache.airavata.common.utils.Constants.GFAC_CONFIG_XML);
        assert resource != null;
        System.out.println(resource.getFile());
        GFacConfiguration gFacConfiguration = GFacConfiguration.create(new File(resource.getPath()), null, null);

        HostDescription host = new HostDescription(Ec2HostType.type);
        host.getType().setHostName("");
        host.getType().setHostAddress("");

        ApplicationDescription ec2Desc = new ApplicationDescription(Ec2ApplicationDeploymentType.type);
        Ec2ApplicationDeploymentType ec2App = (Ec2ApplicationDeploymentType)ec2Desc.getType();

        ServiceDescription serv = new ServiceDescription();
        serv.getType().setName("EC2Test");
        jobExecutionContext=new JobExecutionContext(gFacConfiguration,serv.getType().getName());

        JCloudsSecurityContext securityContext=new JCloudsSecurityContext("ec2-user","aws-ec2","AKIAJ3M3FUZ7PTDQP4YQ","10VE/FvtTuXtehmw/+buEzk3nrS+Kc4uiX+setW+","i-edbb72c6");
        jobExecutionContext.addSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT,securityContext);
    }

    @Test
    public void testInitialize(){
        JCloudsProvider provider=new JCloudsProvider();
        try{
          provider.initialize(jobExecutionContext);
          provider.execute(jobExecutionContext);
        }catch (Exception e){
           System.out.println(e.toString());
        }
    }
}
