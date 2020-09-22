package Application;

import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;

public class Application {

    private static Ec2Client ec2;
    private static S3Client s3;
    private static SqsClient sqs;
    private static final Region region = Region.US_WEST_2;
    private static final String amiId = "ami-0aaf300e7e9b2a831";
    private static String id;
    private static String managerId;

    public static void main(String[] args){
        id = "app" + System.currentTimeMillis();
        s3 = S3Client.builder().credentialsProvider(creds).region(region).build();
        uploadCode();
        s3.createBucket(CreateBucketRequest.builder().bucket(id)
                .createBucketConfiguration(CreateBucketConfiguration.builder()
                                .locationConstraint(region.id()).build()).build());
        ec2 = Ec2Client.builder().credentialsProvider(creds).region(region).build();
        if(!checkManager()) createManager();
        s3.putObject(PutObjectRequest.builder().bucket(id).key("input.txt").build(),
                Paths.get(args[0]));
        String[] queuesURLs = createQueues();
        contactManager(queuesURLs, args[2]);
        outputToHTML(args[1]);

        if(args.length > 3 && args[3].equals("terminate"))
            sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(queuesURLs[0])
                    .messageBody("terminate")
                    .build());
    }

    private static void uploadCode(){
        try {
            s3.createBucket(CreateBucketRequest
                    .builder().bucket("kfirorel")
                    .createBucketConfiguration(CreateBucketConfiguration.builder()
                            .locationConstraint(region.id()).build()).build());
            s3.putObject(PutObjectRequest.builder()
                            .bucket("kfirorel")
                            .key("Manager.zip").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Manager.zip"));
            s3.putObject(PutObjectRequest.builder()
                            .bucket("kfirorel")
                            .key("Worker.zip").acl(ObjectCannedACL.PUBLIC_READ)
                            .build(),
                    Paths.get("Worker.zip"));
        }catch (S3Exception e){ }
    }

    private static boolean checkManager(){
        try {
            if(managerId == null) {
                    List<Reservation> reservations = ec2.describeInstances(DescribeInstancesRequest.builder()
                            .filters(Filter.builder().name("tag:Role").values("Manager").build()).build()).reservations();
                    managerId = null;
                    for (Reservation res : reservations)
                        for (Instance ins : res.instances())
                            if (ins.state().name() == InstanceStateName.RUNNING || ins.state().name() == InstanceStateName.PENDING)
                                managerId = ins.instanceId();
            } else {
                InstanceStateName state = ec2.describeInstances(DescribeInstancesRequest.builder()
                        .instanceIds(managerId).build()).reservations().get(0)
                        .instances().get(0).state().name();
                if(state != InstanceStateName.PENDING && state != InstanceStateName.RUNNING)
                    managerId = null;
            }
        } catch (Exception e) { }
        return managerId != null;
    }

    private static void createManager(){
        RunInstancesResponse response = ec2.runInstances(RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1).minCount(1)
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                .userData(Base64.getEncoder().encodeToString((
                            "#!/bin/bash\n" +
                            "wget https://kfirorel.s3-us-west-2.amazonaws.com/Manager.zip -O Manager.zip\n" +
                            "unzip -P DekelVaknin Manager.zip\n" +
                            "java -jar Manager.jar\n" +
                            "shutdown -h now\n").getBytes()))
                .build());
        managerId = response.instances().get(0).instanceId();

        ec2.createTags(CreateTagsRequest.builder().resources(managerId)
                .tags(Tag.builder().key("Role").value("Manager").build()).build());
    }

    private static String[] createQueues(){
        String[] queuesURLs = new String[2];
        sqs = SqsClient.builder().credentialsProvider(creds).region(region).build();
        try {
            queuesURLs[0] = sqs.createQueue(CreateQueueRequest.builder()
                            .queueName("appTaskQueue")
                            .build()).queueUrl();
            queuesURLs[1] = sqs.createQueue(CreateQueueRequest.builder()
                            .queueName("appDoneTaskQueue")
                            .build()).queueUrl();
        } catch (QueueNameExistsException e) {
            queuesURLs[0] = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName("appTaskQueue").build()).queueUrl();
            queuesURLs[1] = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                    .queueName("appDoneTaskQueue").build()).queueUrl();
        }
        return queuesURLs;
    }

    private static void contactManager(String[] queuesURLs, String n){
        sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queuesURLs[0])
                .messageBody(id + " " + n)
                .build());
        try {
            while (true) {
                int qn = 1;
                List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                        .queueUrl(queuesURLs[1]).visibilityTimeout(0).build()).messages();
                if (messages.size() == 0 && !checkManager()) {
                    messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                            .queueUrl(queuesURLs[0]).visibilityTimeout(0).build()).messages();
                    qn = 0;
                }
                for (Message m : messages) {
                    String[] record = m.body().split(" ");
                    if (record[0].equals(id)) {
                        sqs.deleteMessage(DeleteMessageRequest.builder()
                                .queueUrl(queuesURLs[qn])
                                .receiptHandle(m.receiptHandle())
                                .build());
                        if (qn == 0 || record[1].equals("2"))
                            throw new RuntimeException();
                        else return;
                    }
                }
            }
        }catch (Exception e) {
            System.out.println("Manager is unable to handle request. Please try again in a minute");
            System.exit(1);
        }
    }

    private static void outputToHTML(String fileName){
        ResponseInputStream output = s3.getObject(GetObjectRequest.builder()
                        .bucket(id)
                        .key("output.txt").build(),
                ResponseTransformer.toInputStream());
        StringBuilder result = new StringBuilder("<!DOCTYPE html ><html><head><title>"
                + output + "</title></head><body><div>\n");
        BufferedReader reader = new BufferedReader(new InputStreamReader(output));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] record = line.split("\t");
                result.append("<p>").append("<a href=\"")
                        .append(record[1]).append("\">").append(record[1]).append("</a>\t")
                        .append(record[0].equals("1") ? (record[2] + "\t<a href=\"" + record[3] + "\">"
                                + record[3] + "</a></p>\n") : record[2]);
            }
            result.append("\n</div></body></html>");
        BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
        writer.write(result.toString());
        writer.close();
        }catch (IOException e){}
    }
}
