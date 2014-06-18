package org.apache.airavata.core.gfac.services.impl;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.domain.Location;
import org.jclouds.compute.domain.Image;
import org.jclouds.io.Payload;
import org.jclouds.logging.log4j.config.Log4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Iterator;
import java.util.Set;


/**
 * Created with IntelliJ IDEA.
 * User: Udara
 * Date: 5/3/14
 * Time: 6:33 PM
 * To change this template use File | Settings | File Templates.
 */
public class JcloudsTest {
    final Logger logger= LoggerFactory.getLogger(this.getClass());
    public static void main(String[] args){

        String accessKeyId="AKIAJ3M3FUZ7PTDQP4YQ";
        String secretKeyId="10VE/FvtTuXtehmw/+buEzk3nrS+Kc4uiX+setW+";
        String group="AiravataCluster";

        ComputeServiceContext computeContext= ContextBuilder.newBuilder("aws-ec2")
                                                     .credentials(accessKeyId,secretKeyId)
                                                     .modules(ImmutableSet.<Module> of(new Log4JLoggingModule(),new SshjSshClientModule()))
                                                     .buildView(ComputeServiceContext.class);

      /*  Set<? extends Location> assignableLocations=computeContext.getComputeService().listAssignableLocations();
        Iterator locationIterator=assignableLocations.iterator();
        while(locationIterator.hasNext()){
            System.out.println(locationIterator.next().toString());
        }*/
        Set<? extends ComputeMetadata> nodes=computeContext.getComputeService().listNodes();
        Iterator locationIterator=nodes.iterator();
        while(locationIterator.hasNext()){
            System.out.println(locationIterator.next().toString());
        }

        System.out.println("\n");
        Set<? extends Image> images=computeContext.getComputeService().listImages();
        Iterator imageIterator=images.iterator();
        while(imageIterator.hasNext()){
            System.out.println(imageIterator.next().toString());
        }

        System.out.println("I succeed it \n");

        BlobStoreContext context=ContextBuilder.newBuilder("aws-s3")
                                                .credentials(accessKeyId, secretKeyId)
                                                .buildView(BlobStoreContext.class);
        BlobStore blobStore=context.getBlobStore();
        long count=blobStore.countBlobs("Udara");

        // getting locations to place the buckets
        Set<? extends Location> locations=blobStore.listAssignableLocations();
        Iterator iterator=locations.iterator();
        while(iterator.hasNext()){
           System.out.println(iterator.next().toString());
        }

        // check whether container exist
        boolean exist=blobStore.containerExists("Udara");
        System.out.println("container Udara : "+exist);

        // create a directory within container Udara
        if(!blobStore.directoryExists("Udara","userFiles")){
            blobStore.createDirectory("Udara","userFiles");
        }

        // download blob if exist
        /*if(blobStore.blobExists("Udara","read.txt")){
            Blob downloadFile=blobStore.getBlob("Udara","read.txt");
            Payload payload=downloadFile.getPayload();
            try{
              OutputStream out=new FileOutputStream(new File("C:\\Users\\Udara\\Desktop\\NewRead.txt"));
              payload.writeTo(out);
              payload.close();
              out.flush();
              out.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/

        // creating a new blob
        /*BlobBuilder blobBuilder=blobStore.blobBuilder("song");
        Blob blob=blobBuilder.build();
        blob.setPayload("C:\\Users\\Udara\\Desktop\\song.txt");
        String result=blobStore.putBlob("Udara",blob);
        System.out.println(result);*/
        System.out.println(count);
    }
}
