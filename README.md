gfac-jclouds
============

Upgrade to Airavata Ec2Provider with JClouds intergration

Running Tests:

1. Ec2_credentialTest 

   Start your local AiravataServer
   Make sure the credential store specific details in the airavata-server.properties 
   and the test are same
   provide your ec2 accessKey, secretKey, and ssh publicKeyFile to the node
   Provide gateWayName, setPortalUserName
   Run the test

   This will create a ec2Credential in credential store,use the tokeId shown here to run the other tests

2. JCloudsProviderTestWithURIType

   In this test i have used a simple script name fileMerge.sh to merge two files and write to another file. 
     *userName- ec2 node login username
     *instanceId -ec2 node id
     *user - Portal UserName used to create ec2Credential
     *tockenId- token given when create ec2Credential
     *gatewayId- gateWayName used to create ec2Credential 
     *app executableLocation is the path of the script file
     *app executableType is the script type
     *tempDir- working directory
     *Input1 and Input2 specify the InputParameterType here its URIParameterType
     *inputParam1 and inputParam2 are the ActualParameters which used to specify 
      input File names. 

    Replace all these according to your application and its inputs and outputs
    Run the Test....

     
     

   


