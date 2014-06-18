package org.apache.airavata.gfac.utils;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.google.inject.Module;
import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.RunNodesException;
import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.ssh.SshClient;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.apache.airavata.gfac.security.JCloudsSecurityContext;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_SCRIPT_COMPLETE;
import static org.jclouds.scriptbuilder.domain.Statements.exec;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/29/14
 * Time: 5:35 PM
 * To change this template use File | Settings | File Templates.
 */
public class JCloudsUtils {
    private String jobID = null;
    private String taskID = null;
    private JCloudsSecurityContext securityContext;
    private String nodeId;
    private ComputeServiceContext context;
    private LoginCredentials credentials;

   public static boolean terminateNode(ComputeService service,String nodeId){
       service.destroyNode(nodeId);
       if (checkNodeStateEqual(service,nodeId, NodeMetadata.Status.TERMINATED)){
           return true;
       }
       return false;
   }

   public static boolean checkNodeStateEqual(ComputeService service,String nodeId,NodeMetadata.Status status ){
       NodeMetadata node=service.getNodeMetadata(nodeId);
       if (node.getStatus()==status){
           return true;
       }
       return false;
   }

   public static boolean startNode(ComputeService service,String nodeId){
       service.resumeNode(nodeId);
       if (checkNodeStateEqual(service,nodeId, NodeMetadata.Status.RUNNING)){
           return true;
       }
       return false;
   }

   public static boolean stopNode(ComputeService service,String nodeId){
       service.suspendNode(nodeId);
       if (checkNodeStateEqual(service,nodeId, NodeMetadata.Status.SUSPENDED)){
           return true;
       }
       return false;
   }

    public static ExecResponse runScriptOnNode(LoginCredentials credentials,String nodeId,String command,ComputeService service,boolean runAsRoot){
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

    public static boolean isS3CmdInstall(LoginCredentials credentials,String nodeId,ComputeService service){
        String command="s3cmd --version";
        ExecResponse response=runScriptOnNode(credentials,nodeId,command,service,false);
        if(response.getExitStatus()==12){
            return true;
        }
        return false;
    }

    public static void installS3CmdOnNode(JCloudsSecurityContext securityContext,LoginCredentials credentials,String nodeId,ComputeService service){
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

        runScriptOnNode(credentials,nodeId,installs3cmd,service,true);
        runScriptOnNode(credentials,nodeId,configures3cmd,service,true);
    }

    public void initJCloudsEnvironment(JobExecutionContext jobExecutionContext) throws GFacException {
        if(jobExecutionContext!=null){
            securityContext=(JCloudsSecurityContext)jobExecutionContext.getSecurityContext(JCloudsSecurityContext.JCLOUDS_SECURITY_CONTEXT);
        }

        Properties overides=new Properties();
        long scriptTimeout= TimeUnit.MILLISECONDS.convert(20,TimeUnit.MINUTES);
        overides.setProperty(TIMEOUT_SCRIPT_COMPLETE,scriptTimeout+"");

        Iterable<Module> modules= ImmutableSet.<Module>of(new Log4JLoggingModule(),
                new SshjSshClientModule());
        ContextBuilder builder=ContextBuilder.newBuilder(securityContext.getProviderName())
                .credentials(securityContext.getAccessKey(), securityContext.getSecretKey())
                .modules(modules)
                .overrides(overides);
        this.context= builder.buildView(ComputeServiceContext.class);
        ComputeService service=context.getComputeService();

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
            JCloudsUtils.startNode(service,nodeId);
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
