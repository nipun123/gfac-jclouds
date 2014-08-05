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

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.credential.store.credential.Credential;
import org.apache.airavata.credential.store.credential.impl.ec2.Ec2Credential;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.CredentialStoreException;
import org.apache.airavata.gfac.AbstractSecurityContext;
import org.apache.airavata.gfac.RequestData;
import org.apache.airavata.gfac.SecurityContext;
import org.apache.airavata.gfac.core.utils.GFacUtils;
import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.UnsupportedEncodingException;

public class JCloudsSecurityContext implements  SecurityContext{
    private static final Logger log = LoggerFactory.getLogger(JCloudsSecurityContext.class);

    public static final String JCLOUDS_SECURITY_CONTEXT="jclouds";


    private String accessKey;
    private String secretKey;
    private String amiId;
    private String instanceType;
    private String nodeId;
    private String publicKey;
    private String userName;
    private String providerName;
    private CredentialReader credentialReader;
    private RequestData requestData;

    public JCloudsSecurityContext(String userName,String providerName,String nodeId,CredentialReader credentialReader,RequestData requestData){
        this.nodeId=nodeId;
        this.userName=userName;
        this.providerName=providerName;
        this.credentialReader=credentialReader;
        this.requestData=requestData;
    }

    public void getCredentialsFromStore() {
        if(getCredentialReader() ==null){
            try {
                setCredentialReader(GFacUtils.getCredentialReader());
            } catch (ApplicationSettingsException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (InstantiationException e) {
                e.printStackTrace();
            }
        }

        Credential credential=null;
        try {
            credential=credentialReader.getCredential(requestData.getGatewayId(), requestData.getTokenId());
        } catch (CredentialStoreException e) {
            e.printStackTrace();
        }

        if(credential!=null){
            if(credential instanceof Ec2Credential){
                log.info("Successfully retrive the credentials for gatewayId "+requestData.getGatewayId()+" " +
                        "and tokenId "+requestData.getTokenId());

                Ec2Credential ec2Credential=(Ec2Credential)credential;
                accessKey=ec2Credential.getAccessKey();
                secretKey=ec2Credential.getSecretKey();
                try {
                    publicKey=new String(ec2Credential.getPublickey(), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }

            }
            else{
                log.info("credential type for the gatewayId "+requestData.getGatewayId()+" and tokenId "+
                 requestData.getTokenId()+" is not Ec2CredentialType");
            }

        }else{
            log.info("Credential for the gateway "+requestData.getGatewayId()+" and tokenId "+requestData.getTokenId()+
             " cannot be found");
        }

    }

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

    public String getNodeId() {
        return nodeId;
    }

    public String getPublicKey() {
        return publicKey;
    }

    public void setPublicKey(String publicKey) {
        this.publicKey = publicKey;
    }

    public String getUserName() {
        return userName;
    }

    public CredentialReader getCredentialReader() {
        return credentialReader;
    }

    public void setCredentialReader(CredentialReader credentialReader) {
        this.credentialReader = credentialReader;
    }

    public RequestData getRequestData() {
        return requestData;
    }

    public void setRequestData(RequestData requestData) {
        this.requestData = requestData;
    }
}

