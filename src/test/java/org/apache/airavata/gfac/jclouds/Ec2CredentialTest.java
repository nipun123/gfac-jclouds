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

package org.apache.airavata.gfac.jclouds;

import org.apache.airavata.common.exception.ApplicationSettingsException;
import org.apache.airavata.common.utils.DBUtil;
import org.apache.airavata.credential.store.credential.Credential;
import org.apache.airavata.credential.store.credential.impl.ec2.Ec2Credential;
import org.apache.airavata.credential.store.store.CredentialReader;
import org.apache.airavata.credential.store.store.CredentialStoreException;
import org.apache.airavata.credential.store.store.impl.CredentialReaderImpl;
import org.apache.airavata.credential.store.store.impl.Ec2CredentialWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.constraints.AssertFalse;
import java.sql.SQLException;
import java.util.UUID;

public class Ec2CredentialTest {
    DBUtil dbUtil;
    String accessKey="";
    String secretKey="";

    @Before
    public void setupCredentialStore() throws IllegalAccessException, ClassNotFoundException, InstantiationException, SQLException {
        System.setProperty("credential.store.keystore.url", "/usr/local/AiravataNewProject/airavata/modules/configuration/server/src/main/resources/airavata.jks");
        System.setProperty("credential.store.keystore.alias", "airavata");
        System.setProperty("credential.store.keystore.password", "airavata");
        System.setProperty("credential.store.jdbc.url","jdbc:derby://localhost:1527/credential_store;create=true;user=admin;password=admin");
        System.setProperty("credential.store.jdbc.user","admin");
        System.setProperty("credential.store.jdbc.password","admin");
        System.setProperty("credential.store.jdbc.driver","org.apache.derby.jdbc.ClientDriver");

        dbUtil=new DBUtil("jdbc:derby://localhost:1527/credential_store;create=true;user=admin;password=admin",
                "admin", "admin", "org.apache.derby.jdbc.ClientDriver");


        String createTable = "CREATE TABLE CREDENTIALS\n" + "(\n"
                + "        GATEWAY_ID VARCHAR(256) NOT NULL,\n"
                + "        TOKEN_ID VARCHAR(256) NOT NULL,\n"
                + // Actual token used to identify the credential
                "        CREDENTIAL BLOB NOT NULL,\n" + "PORTAL_USER_ID VARCHAR(256) NOT NULL,\n"
                + "        TIME_PERSISTED TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n"
                + "        PRIMARY KEY (GATEWAY_ID, TOKEN_ID)\n" + ")";

        String dropTable = "drop table CREDENTIALS";

        try {
            dbUtil.executeSQL(dropTable);
        } catch (Exception e) {
        }

        dbUtil.executeSQL(createTable);

    }

    @Test
    public void testEc2CredentialWriter() throws ApplicationSettingsException {

        // create ec2 credential
        Ec2Credential credential=new Ec2Credential(accessKey,secretKey,null,"gateway123","ec2-user");
        UUID uuid = UUID.randomUUID();
        System.out.println("TokenId: " + uuid.toString());
        credential.setToken(uuid.toString());
        credential.setPortalUserName("user123");

        // write credential
        Ec2CredentialWriter writer=new Ec2CredentialWriter(dbUtil);
        try {
            writer.writeCredentials(credential);
        } catch (CredentialStoreException e) {
            e.printStackTrace();
        }

        // Read the ec2 credential from credential store
        CredentialReader credentialReader = new CredentialReaderImpl(dbUtil);
        Credential credential1=null;
        try {
            credential1=credentialReader.getCredential("gateway123", uuid.toString());
        } catch (CredentialStoreException e) {
            e.printStackTrace();
        }

        if(credential1 instanceof Ec2Credential){
            Assert.assertEquals(accessKey,((Ec2Credential)credential).getAccessKey());
            Assert.assertEquals(secretKey, ((Ec2Credential) credential).getSecretKey());
        }else{
            Assert.fail();
        }
    }
}
