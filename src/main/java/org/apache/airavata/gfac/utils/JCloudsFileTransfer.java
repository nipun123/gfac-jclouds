package org.apache.airavata.gfac.utils;

import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.io.ByteSources;
import org.jclouds.io.Payload;
import org.jclouds.io.payloads.ByteSourcePayload;
import org.jclouds.ssh.SshClient;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 6/12/14
 * Time: 1:32 PM
 * To change this template use File | Settings | File Templates.
 */
public class JCloudsFileTransfer {
    private SshClient ssh;

    public JCloudsFileTransfer(ComputeServiceContext context,String nodeId,LoginCredentials credentials){
        NodeMetadata node=context.getComputeService().getNodeMetadata(nodeId);
        ssh=context.utils().sshForNode().apply(NodeMetadataBuilder.fromNodeMetadata(node)
                .credentials(credentials).build());
    }

    public void uploadFileToEc2(String remoteFile,String localFile){
        try{
          ssh.connect();
          File file=new File(localFile);
          ByteSource byteSource= Files.asByteSource(file);
          Payload payload=new ByteSourcePayload(byteSource);
          payload.getContentMetadata().setContentLength(byteSource.size());
          ssh.put(remoteFile, payload);

        }catch (Exception e){
           e.printStackTrace();
        }finally {
            if (ssh!=null){
                ssh.disconnect();
            }
        }

    }

    public void downloadFilesFromEc2(String localFile,String remoteFile){
        FileOutputStream ops;
        try{
          ssh.connect();
          ops=new FileOutputStream(new File(localFile));
          Payload payload=ssh.get(remoteFile);
          InputStream inputStream=payload.openStream();
          byte[] bytes= ByteStreams.toByteArray(inputStream);
          ByteSource byteSource=ByteSource.wrap(bytes);
          byteSource.copyTo(ops);
          inputStream.close();
          ops.close();
       }catch (Exception e){
          e.printStackTrace();
       }finally
        {
           if(ssh!=null)
               ssh.disconnect();
        }
    }
}
