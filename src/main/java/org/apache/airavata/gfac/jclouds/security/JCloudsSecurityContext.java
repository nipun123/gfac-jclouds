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
package org.apache.airavata.gfac.jclouds.security;

import org.apache.airavata.gfac.SecurityContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class JCloudsSecurityContext implements SecurityContext{
    private static final Logger log = LoggerFactory.getLogger(JCloudsSecurityContext.class);

    public static final String JCLOUDS_SECURITY_CONTEXT="jclouds";

    private String accessKey;
    private String secretKey;
    private String amiId;
    private String instanceType;
    private String nodeId;
    private String keyPair;
    private String securityGroup;
    private File publicKey;
    private File privateKey;
    private String userName;
    private NodeMetadata.Status status;
    private String providerName;

    public String getProviderName() {
        return providerName;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public String getInstanceType() {
        return instanceType;
    }

    public String getAmiId() {
        return amiId;
    }

    public String getKeyPair() {
        return keyPair;
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getSecurityGroup() {
        return securityGroup;
    }

    public File getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = new File(publicKey);
    }

    public String getUserName() {
        return userName;
    }

    public File getPrivateKey() {
        return privateKey;
    }

    public void setPrivateKey(String privateKey) {
        this.privateKey = new File(privateKey);
    }

    public void setStatus(NodeMetadata.Status status) {
        this.status = status;
    }

    public NodeMetadata.Status getStatus() {
        return status;
    }

    public JCloudsSecurityContext(String userName,String providerName,String accessKey,String secretKey,String nodeId){
        this.accessKey=accessKey;
        this.secretKey=secretKey;
        this.nodeId=nodeId;
        this.userName=userName;
        this.providerName=providerName;
    }
}
