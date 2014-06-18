package org.apache.airavata.gfac.provider.impl;

import com.google.common.collect.ImmutableSet;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Module;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.provider.AbstractProvider;
import org.apache.airavata.gfac.core.provider.GFacProviderException;
import org.apache.airavata.gfac.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.utils.JCloudsUtils;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.*;

import static com.google.common.collect.Iterables.removeAll;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.io.ByteSource.wrap;
import static org.jclouds.compute.config.ComputeServiceProperties.TIMEOUT_SCRIPT_COMPLETE;

import org.jclouds.domain.LoginCredentials;
import static com.google.common.collect.Iterables.getOnlyElement;

import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.io.payloads.ByteSourcePayload;
import org.jclouds.io.payloads.InputStreamPayload;
import org.jclouds.io.payloads.StringPayload;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;

import java.io.*;

import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import security.JCloudsSecurityContext;

import static com.google.common.base.Charsets.UTF_8;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/27/14
 * Time: 10:29 AM
 * To change this template use File | Settings | File Templates.
 */
public class JCloudsProvider extends AbstractProvider {
    private static final Logger log = LoggerFactory.getLogger(JCloudsProvider.class);

    private String jobID = null;
    private String taskID = null;
    private JCloudsSecurityContext securityContext;
    private String nodeId;
    private ComputeService service;
    private ComputeServiceContext context;
    private LoginCredentials credentials;

    public void initialize(JobExecutionContext jobExecutionContext) throws GFacException, GFacProviderException {
        super.initialize(jobExecutionContext);
        JCloudsFileTransfer fileTransfer=new JCloudsFileTransfer(context,nodeId,credentials);
    }

    @Override
    public void initProperties(Map<String, String> properties) throws GFacProviderException, GFacException {
    }



    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void dispose(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void cancelJob(String jobId, JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    public void buildCommand(){

    }
}
