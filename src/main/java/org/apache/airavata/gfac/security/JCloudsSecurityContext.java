package org.apache.airavata.gfac.security;

import org.apache.airavata.gfac.SecurityContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/29/14
 * Time: 4:53 PM
 * To change this template use File | Settings | File Templates.
 */
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
