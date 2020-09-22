package Manager;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;

public class InputTask implements Runnable {

    private String app_id;
    private int filesPerWorker;

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;

    private ManagerData data;

    public InputTask(String app_id, int filesPerWorker, ManagerData data){
        this.app_id = app_id;
        this.filesPerWorker = filesPerWorker;
        this.data = data;
        ec2 = data.ec2Clients.poll();
        if(ec2 == null)
            ec2 = Ec2Client.builder().credentialsProvider(data.creds).region(data.region).build();
        s3 = data.s3Clients.poll();
        if(s3 == null)
            s3 = S3Client.builder().credentialsProvider(data.creds).region(data.region).build();
        sqs = data.sqsClients.poll();
        if(sqs == null)
            sqs = SqsClient.builder().credentialsProvider(data.creds).region(data.region).build();
    }

    @Override
    public void run() {
        String newPDFTaskQueue = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(app_id + "_in")
                .build()).queueUrl();
        String donePDFTaskQueue = sqs.createQueue(CreateQueueRequest.builder()
                .queueName(app_id + "_out")
                .build()).queueUrl();

        ResponseInputStream input = s3.getObject(GetObjectRequest.builder()
                        .bucket(app_id)
                        .key("input.txt").build());
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        int count = 0;
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] record = line.split("\t");
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(newPDFTaskQueue)
                        .messageBody(count + " " + record[1] + " " + record[0])
                        .build());
                count++;
            }
            int workers = (int)Math.ceil((double)count / filesPerWorker);

            RunInstancesResponse response = ec2.runInstances(RunInstancesRequest.builder()
                    .imageId(data.amiId)
                    .instanceType(InstanceType.T2_MICRO)
                    .maxCount(workers).minCount(workers)
                    .userData(Base64.getEncoder().encodeToString((
                        "#!/bin/bash\n" +
                        "wget https://kfirorel.s3-us-west-2.amazonaws.com/Worker.zip -O Worker.zip\n" +
                        "unzip -P DekelVaknin Worker.zip\n" +
                        "java -jar Worker.jar " + app_id + " " + newPDFTaskQueue + " " + donePDFTaskQueue + "\n").getBytes()))
                    .build());
            ArrayList<String> instanceIds = new ArrayList<>();
            for(Instance instance : response.instances())
                instanceIds.add(instance.instanceId());
            ec2.createTags(CreateTagsRequest.builder().resources(instanceIds)
                .tags(Tag.builder().key("Role").value("Worker").build()).build());

            data.subManagers.submit(new OutputTask(app_id, count, instanceIds, newPDFTaskQueue, donePDFTaskQueue, data));
            data.ec2Clients.add(ec2);
            data.s3Clients.add(s3);
            data.sqsClients.add(sqs);

        }catch (IOException e){}
    }

}
