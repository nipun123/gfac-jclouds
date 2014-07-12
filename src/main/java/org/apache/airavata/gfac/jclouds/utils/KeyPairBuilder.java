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
package org.apache.airavata.gfac.jclouds.utils;

import net.schmizz.sshj.common.Base64;
import org.bouncycastle.openssl.PEMWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.security.*;
import java.security.spec.InvalidKeySpecException;

public class KeyPairBuilder {
    private static final Logger log = LoggerFactory.getLogger(KeyPairBuilder.class);
    private static KeyPairBuilder keyPairBuilder;
    private File  privateKeyFile;
    private File  publicKeyFile;
    private String keyType;
    private String keyPairName="ec2_rsa";

    public static KeyPairBuilder getInstance(){
        if (keyPairBuilder==null){
            keyPairBuilder=new KeyPairBuilder();
        }
        return keyPairBuilder;
    }

    public KeyPairBuilder(){
        String privateKeyFilePath=System.getProperty("user.home")+"/.ssh/"+keyPairName+".pem";
        String publicKeyFilePath=System.getProperty("user.home")+"/.ssh/"+keyPairName+".pub";
        privateKeyFile=new File(privateKeyFilePath);
        publicKeyFile=new File(publicKeyFilePath);
    }

    public void buildNewKeyPair() throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {

       if(!publicKeyFile.exists()){
          File sshDir=new File(System.getProperty("user.home")+"/.ssh/");
          if (!sshDir.exists()){
              sshDir.mkdir();
          }

          KeyPairGenerator keyPairGenerator=KeyPairGenerator.getInstance("RSA");
          keyPairGenerator.initialize(1024);
          KeyPair keyPair=keyPairGenerator.generateKeyPair();
          Key publicKey=keyPair.getPublic();
          Key privateKey=keyPair.getPrivate();

          // saving public key
          FileOutputStream fos=null;
          try{
             fos=new FileOutputStream(publicKeyFile);
             fos.write(Base64.encodeBytes(publicKey.getEncoded()).getBytes());
          }catch (Exception e){
              System.out.println("Error occured in saving public Key");
          }finally {
              fos.close();
          }

          // store private key
          try {
              fos=new FileOutputStream(privateKeyFile);
              StringWriter writer=new StringWriter();

              PEMWriter pemWriter=new PEMWriter(writer);
              pemWriter.writeObject(privateKey);
              pemWriter.close();
              fos.write(writer.toString().getBytes());
          }catch (Exception e){
             System.out.println("error occured while saving private key");
          }finally {
              fos.close();
          }

           privateKeyFile.setWritable(false, false);
           privateKeyFile.setExecutable(false, false);
           privateKeyFile.setReadable(false, false);
           privateKeyFile.setReadable(true);
           privateKeyFile.setWritable(true);

           keyType="rsa";

       }

    }

    public boolean validatePrivateKeyFile(){
        if (!privateKeyFile.exists()){
           log.info("privateKey file do not exist");
           return false;
        }
        return true;
    }

    public boolean validatePublicKeyFile(){
        if (!publicKeyFile.exists()){
           log.info("publicKey file do not exist");
           return false;
        }
        return true;
    }

    public String getKeyType() {
        return keyType;
    }

    public File getPublicKeyFile() {
        return publicKeyFile;
    }

    public File getPrivateKeyFile() {
        return privateKeyFile;
    }
}
