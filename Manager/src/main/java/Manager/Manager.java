package Manager;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class ManagerData{
    final ExecutorService subManagers = Executors.newFixedThreadPool(10);
    final int BUFFER_ALLOCATION_SIZE = 1 << 18; //256KB
    final ConcurrentLinkedQueue<ByteBuffer> BUFFER_POOL = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<String> runningTasksQueue = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<String> doneTasksQueue = new ConcurrentLinkedQueue<>();
    String appTaskQueue;
    String appDoneTaskQueue;
    final ConcurrentLinkedQueue<Ec2Client> ec2Clients = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<S3Client> s3Clients = new ConcurrentLinkedQueue<>();
    final ConcurrentLinkedQueue<SqsClient> sqsClients = new ConcurrentLinkedQueue<>();

    final AwsCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials
            .create("AKIAJMRQRPUHJUHARONQ",
                    "Fb/PKNQhzkTKjMMf1og/hZGNPiJv/X/7o4vfFczx"));
    final Region region = Region.US_WEST_2;
    final String amiId = "ami-0aaf300e7e9b2a831";
}

public class Manager {

    private static final ManagerData data = new ManagerData();

    public static void main(String[] args) {
        SqsClient sqs = SqsClient.builder().credentialsProvider(data.creds).region(data.region).build();
        data.appTaskQueue = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName("appTaskQueue").build()).queueUrl();
        data.appDoneTaskQueue = sqs.getQueueUrl(GetQueueUrlRequest.builder()
                .queueName("appDoneTaskQueue").build()).queueUrl();
        while (!Thread.currentThread().isInterrupted()) {
            List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(data.appTaskQueue).maxNumberOfMessages(5)
                    .waitTimeSeconds(2)
                    .build()).messages();
            for (Message m : messages) {
                String[] record = m.body().split(" ");
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(data.appTaskQueue)
                        .receiptHandle(m.receiptHandle())
                        .build());
                if (record[0].equals("terminate"))
                    Thread.currentThread().interrupt();
                else {
                    data.runningTasksQueue.add(record[0]);
                    data.subManagers.submit(new InputTask(record[0], Integer.parseInt(record[1]), data));
                }
            }
            for (int i = 0; i < 5; i++) {
                String curr = data.doneTasksQueue.poll();
                if (curr != null)
                    sqs.sendMessage(SendMessageRequest.builder()
                            .queueUrl(data.appDoneTaskQueue)
                            .messageBody(curr + " 1")
                            .build());
                else break;
            }
        }
        while (!data.runningTasksQueue.isEmpty()) { }
        data.subManagers.shutdown();
        try { data.subManagers.awaitTermination(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) { }

        while (!data.doneTasksQueue.isEmpty()) {
            String curr = data.doneTasksQueue.poll();
            sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(data.appDoneTaskQueue)
                .messageBody(curr + " 1")
                .build());
        }
        List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(data.appTaskQueue).build()).messages();
        for (Message m : messages) {
            String[] record = m.body().split(" ");
            if (!record[0].equals("terminate"))
                sqs.sendMessage(SendMessageRequest.builder()
                    .queueUrl(data.appDoneTaskQueue)
                    .messageBody(record[0] + " 2")
                    .build());
            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(data.appTaskQueue)
                    .receiptHandle(m.receiptHandle())
                    .build());
        }
        try { Object o = new Object();
            o.wait(10000); } catch (InterruptedException e) { }
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(data.appTaskQueue).build());
        sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(data.appDoneTaskQueue).build());
    }

}