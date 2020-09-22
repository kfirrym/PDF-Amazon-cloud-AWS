package Manager;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.DeleteQueueRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class OutputTask implements Runnable{

    private String app_id;
    private int recordsLeft;

    private Ec2Client ec2;
    private S3Client s3;
    private SqsClient sqs;
    private List<String> workers;
    private String newPDFTaskQueue;
    private String donePDFTaskQueue;
    private ManagerData data;
    private ByteBuffer buffer;

    public OutputTask(String app_id, int totalRecords, List<String> workers,
                      String newPDFTaskQueue, String donePDFTaskQueue, ManagerData data){
        this.app_id = app_id;
        this.recordsLeft = totalRecords;
        this.workers = workers;
        this.newPDFTaskQueue = newPDFTaskQueue;
        this.donePDFTaskQueue = donePDFTaskQueue;
        this.data = data;
        buffer = data.BUFFER_POOL.poll();
        if (buffer == null)
            buffer = ByteBuffer.allocateDirect(data.BUFFER_ALLOCATION_SIZE);
        buffer.clear();
    }

    private void getClients(){
        ec2 = data.ec2Clients.poll();
        if(ec2 == null)
            ec2 = Ec2Client.builder().credentialsProvider(data.creds).build();
        s3 = data.s3Clients.poll();
        if(s3 == null)
            s3 = S3Client.builder().credentialsProvider(data.creds).region(data.region).build();
        sqs = data.sqsClients.poll();
        if(sqs == null)
            sqs = SqsClient.builder().credentialsProvider(data.creds).region(data.region).build();
    }

    @Override
    public void run() {
        getClients();
        List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(donePDFTaskQueue).maxNumberOfMessages(10)
                .waitTimeSeconds(2)
                .build()).messages();
        FileOutputStream out = null;
        for (Message m : messages) {
            recordsLeft--;
            byte[] recordBytes = m.body().getBytes();
            if(out == null){
                if(recordsLeft == 0 || buffer.remaining() < recordBytes.length){
                    try{
                        s3.getObject(GetObjectRequest.builder().bucket(app_id)
                            .key("output.txt").build(),
                            ResponseTransformer.toFile(Paths.get(app_id)));
                    } catch (Exception e){
                        try { new File(app_id).createNewFile(); } catch (IOException e1) {}
                    }
                    try{
                        out = new FileOutputStream(app_id,true);
                        buffer.flip();
                        out.getChannel().write(buffer);
                        out.write(recordBytes);
                        buffer.clear();
                    }catch (IOException e){}
                }else
                    buffer.put(recordBytes);
            }else
                try { out.write(recordBytes); }catch (IOException e){}
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(donePDFTaskQueue)
                    .receiptHandle(m.receiptHandle())
                    .build());
        }
        if(out != null) {
            s3.putObject(PutObjectRequest.builder().bucket(app_id).key("output.txt").build(), Paths.get(app_id));
            new File(app_id).delete();
        }
        if(messages.size() == 0) manageWorkers();

        data.ec2Clients.add(ec2);
        data.s3Clients.add(s3);
        data.sqsClients.add(sqs);

        if(recordsLeft == 0) {
            data.BUFFER_POOL.add(buffer);
            data.runningTasksQueue.remove(app_id);
            data.doneTasksQueue.add(app_id);
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(workers).build());
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(newPDFTaskQueue).build());
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(donePDFTaskQueue).build());
        } else
            data.subManagers.submit(this);
    }

    private void manageWorkers(){
        try {
            List<Reservation> reservations = ec2.describeInstances(DescribeInstancesRequest.builder()
                    .instanceIds(workers).build()).reservations();
            for (Reservation res : reservations)
                for (Instance ins : res.instances())
                    if (ins.state().name() != InstanceStateName.RUNNING && ins.state().name() != InstanceStateName.PENDING) {
                        workers.remove(ins.instanceId());
                        RunInstancesResponse response = ec2.runInstances(RunInstancesRequest.builder()
                                .imageId(data.amiId)
                                .instanceType(InstanceType.T2_MICRO)
                                .maxCount(1).minCount(1)
                                .userData(Base64.getEncoder().encodeToString((
                                        "#!/bin/bash\n" +
                                                "wget https://kfirorel.s3-us-west-2.amazonaws.com/Worker.zip -O Worker.zip\n" +
                                                "unzip -P DekelVaknin Worker.zip\n" +
                                                "java -jar Worker.jar " + app_id + " " + newPDFTaskQueue + " " + donePDFTaskQueue + "\n").getBytes()))
                                .build());
                        String worker = response.instances().get(0).instanceId();
                        ec2.createTags(CreateTagsRequest.builder().resources(worker)
                                .tags(Tag.builder().key("Role").value("Worker").build()).build());
                        workers.add(worker);
                    }
        }catch (Exception e) {}
    }

}
