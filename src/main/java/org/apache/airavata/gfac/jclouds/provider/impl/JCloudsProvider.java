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
package org.apache.airavata.gfac.jclouds.provider.impl;

import org.apache.airavata.commons.gfac.type.ActualParameter;
import org.apache.airavata.commons.gfac.type.ApplicationDescription;
import org.apache.airavata.commons.gfac.type.MappingFactory;
import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.context.MessageContext;
import org.apache.airavata.gfac.core.provider.AbstractProvider;
import org.apache.airavata.gfac.core.provider.GFacProviderException;
import org.apache.airavata.gfac.jclouds.utils.JCloudsFileTransfer;
import org.apache.airavata.gfac.jclouds.utils.JCloudsUtils;
import org.apache.airavata.schemas.gfac.Ec2ApplicationDeploymentType;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;

import org.jclouds.compute.domain.ExecResponse;
import org.jclouds.domain.LoginCredentials;
import static com.google.common.collect.Iterables.getOnlyElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.airavata.gfac.jclouds.security.JCloudsSecurityContext;

import java.util.Map;
import java.util.Set;

public class JCloudsProvider extends AbstractProvider {
    private static final Logger log = LoggerFactory.getLogger(JCloudsProvider.class);

    private String jobID = null;
    private String taskID = null;
    private JCloudsSecurityContext securityContext;
    private String nodeId;
    private ComputeService service;
    private ComputeServiceContext context;
    private LoginCredentials credentials;
    private JCloudsUtils jCloudsUtils;


    public void initialize(JobExecutionContext jobExecutionContext) throws GFacException, GFacProviderException {
        if(jobExecutionContext==null) {
            throw new GFacException("jobexecution context is null");
        }
        super.initialize(jobExecutionContext);
        jCloudsUtils=JCloudsUtils.getInstance();
        credentials=jCloudsUtils.getCredentials();
    }


    @Override
    public void execute(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
        String command=buildCommand(jobExecutionContext);
        ExecResponse response=jCloudsUtils.runScriptOnNode(credentials,command,true);
        
    }


    public String buildCommand(JobExecutionContext context){
        ApplicationDescription ec2App=jobExecutionContext.getApplicationContext().getApplicationDeploymentDescription();
        Ec2ApplicationDeploymentType app=(Ec2ApplicationDeploymentType)ec2App.getType();
        String SPACE=" ";
        StringBuffer cmd=new StringBuffer();
        cmd.append(app.getExecutableType());
        cmd.append(SPACE);
        cmd.append(app.getExecutableLocation());

        MessageContext input=context.getInMessageContext();
        Map<String, Object> inputs = input.getParameters();
        Set<String> keys = inputs.keySet();
        for(String paraName :keys){
            ActualParameter actualParameter=(ActualParameter)input.getParameter(paraName);
            String paramValue=MappingFactory.toString(actualParameter);
            cmd.append(paramValue);
            cmd.append(SPACE);
        }
        return cmd.toString();

    }

    @Override
    public void dispose(JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void cancelJob(String jobId, JobExecutionContext jobExecutionContext) throws GFacProviderException, GFacException {
    }

    @Override
    public void initProperties(Map<String, String> properties) throws GFacProviderException, GFacException {
    }
}
