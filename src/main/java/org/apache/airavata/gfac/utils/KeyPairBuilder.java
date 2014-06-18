package org.apache.airavata.gfac.utils;


import net.iharder.base64.Base64;
import org.bouncycastle.openssl.PEMWriter;

import java.io.*;
import java.security.*;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 6/2/14
 * Time: 11:47 AM
 * To change this template use File | Settings | File Templates.
 */
public class KeyPairBuilder {
    private static KeyPairBuilder keyPairBuilder;
    private File  privateKeyFile;
    private File  publicKeyFile;
    private String keyType;

    public static KeyPairBuilder getInstance(){
        if (keyPairBuilder==null){
            keyPairBuilder=new KeyPairBuilder();
        }
        return keyPairBuilder;
    }

    public void buildKeyPair(String keyPairName) throws NoSuchAlgorithmException, InvalidKeySpecException, IOException {
       String privateKeyFilePath=System.getProperty("user.home")+"/.ssh/"+keyPairName+".pem";
       String publicKeyFilePath=System.getProperty("user.home")+"/.ssh/"+keyPairName+".pub";
       privateKeyFile=new File(privateKeyFilePath);
       publicKeyFile=new File(publicKeyFilePath);

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
        if (privateKeyFile==null){
           System.out.println("no privateKey file defined");
           return false;
        }
        if (!privateKeyFile.exists()){
           System.out.println("privateKey file do not exist");
           return false;
        }
        return true;
    }

    public boolean validatePublicKeyFile(){
        if (publicKeyFile==null){
           System.out.println("no publicKey file defined");
           return false;
        }
        if (!publicKeyFile.exists()){
           System.out.println("publicKey file do not exist");
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
