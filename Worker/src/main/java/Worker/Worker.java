package Worker;

import org.apache.pdfbox.pdmodel.*;
import org.apache.pdfbox.rendering.*;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.awt.image.*;
import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.nio.file.Paths;
import java.util.List;
import javax.imageio.*;

public class Worker {

    private static final Region region = Region.US_WEST_2;
    private static String app_id;
    private static S3Client s3;
    private static SqsClient sqs;
    private static String task_queue_url;
    private static String completion_queue_url;

    public static void main(String[] args){
        app_id = args[0];
        task_queue_url = args[1];
        completion_queue_url = args[2];
        sqs = SqsClient.builder().credentialsProvider(creds).region(region).build();
        s3 = S3Client.builder().credentialsProvider(creds).region(region).build();
        while (!Thread.currentThread().isInterrupted()) {
            List<Message> messages = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(task_queue_url)
                    .maxNumberOfMessages(1)
                    .visibilityTimeout(15)
                    .build()).messages();
            if(messages.size() == 0)
                continue;
            String[] content = messages.get(0).body().split(" "); //rec_num link operation
            String result = "1\t";
            try{
                String fileEnd = processPDF(content[1], content[2]);
                s3.putObject(PutObjectRequest.builder()
                        .bucket(app_id)
                        .key(content[0] + fileEnd).acl(ObjectCannedACL.PUBLIC_READ)
                        .build(), Paths.get("out" + fileEnd));
                String objectUrl = "https://" + app_id + ".s3-us-west-2.amazonaws.com/" + content[0] + fileEnd;
                result += content[1] + "\t" + content[2] + "\t" + objectUrl + "\n";
                File out = new File("out" + fileEnd);
                out.delete();
            }catch (Exception e){
                result = "2\t" + content[1] + "\t" + e.getMessage() + "\n";
            }finally {
                sqs.deleteMessage(DeleteMessageRequest.builder()
                        .queueUrl(task_queue_url)
                        .receiptHandle(messages.get(0).receiptHandle())
                        .build());
                sqs.sendMessage(SendMessageRequest.builder()
                        .queueUrl(completion_queue_url)
                        .messageBody(result)
                        .build());
            }
        }
    }

    private static String processPDF(String link, String operation) throws Exception{
        URLConnection url = new URL(link).openConnection();
        url.setConnectTimeout(5000);
        url.setReadTimeout(5000);
        PDDocument pd = PDDocument.load(url.getInputStream());
        String fileEnd = "";
        switch (operation) {
            case "ToHTML":
            case "ToText":
                fileEnd = (operation.equals("ToHTML")) ? ".html" : ".txt";
                PDFTextStripper pdfStripper = (operation.equals("ToHTML")) ? new PDFText2HTML() : new PDFTextStripper();
                pdfStripper.setStartPage(1);
                pdfStripper.setEndPage(1);
                BufferedWriter writer = new BufferedWriter(new FileWriter("out" + fileEnd));
                writer.write(pdfStripper.getText(pd));
                writer.close();
                break;
            case "ToImage":
                fileEnd = ".png";
                PDFRenderer pr = new PDFRenderer (pd);
                BufferedImage bi = pr.renderImageWithDPI (0, 300);
                ImageIO.write (bi, "PNG", new File ("out" + fileEnd));
        }
        pd.close();
        return fileEnd;
    }

}
