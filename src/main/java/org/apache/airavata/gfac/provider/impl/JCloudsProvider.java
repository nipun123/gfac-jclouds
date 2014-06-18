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
package org.apache.airavata.gfac.provider.impl;

import org.apache.airavata.gfac.GFacException;
import org.apache.airavata.gfac.core.context.JobExecutionContext;
import org.apache.airavata.gfac.core.provider.AbstractProvider;
import org.apache.airavata.gfac.core.provider.GFacProviderException;
import org.apache.airavata.gfac.utils.JCloudsFileTransfer;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;

import org.jclouds.domain.LoginCredentials;
import static com.google.common.collect.Iterables.getOnlyElement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.airavata.gfac.security.JCloudsSecurityContext;

import java.util.Map;

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
