package com.github.oliveiras.aws.samples.sqs;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Example showing how to send and receive a simple text message using SQS.
 *
 * @author Alex Oliveira
 */
public class SendAndReceive {

    private static Logger log = LoggerFactory.getLogger(SendAndReceive.class);

    /**
     * Entry-point of the example.
     */
    public static void main(String[] args) {
        // Create command line options.
        Options options = new Options()
                .addOption(Option.builder("a").longOpt("access-key").hasArg().required().desc("AWS Access Key").build())
                .addOption(Option.builder("s").longOpt("secret-key").hasArg().required().desc("AWS Secret Key").build())
                .addOption(Option.builder("r").longOpt("aws-region").hasArg().required().desc("AWS region").build())
                .addOption(Option.builder("q").longOpt("queue-name").hasArg().required().desc("SQS queue name").build());

        try {
            // Parse command line arguments.
            CommandLineParser parser = new DefaultParser();
            CommandLine commands = parser.parse(options, args);
            String awsAccessKey = commands.getOptionValue("a");
            String awsSecretKey = commands.getOptionValue("s");
            String awsRegion = commands.getOptionValue("r");
            String queueName = commands.getOptionValue("q");
            log.info("Command line arguments parsed.");

            // Test SQS.
            sendReceive(awsAccessKey, awsSecretKey, awsRegion, queueName);
            log.info("Example completed.");

        } catch (ParseException e) {
            new HelpFormatter().printHelp("send-receive", options);
        } catch (Exception e) {
            log.error("Could not complete the example.", e);
        }
    }

    /**
     * Send and receive a simples message.
     *
     * @param awsAccessKey ID of the access-key used to interact with SQS.
     * @param awsSecretKey Secret part of the access-key.
     * @param awsRegion Region where the queue was created.
     * @param queueName Name of the queue.
     */
    private static void sendReceive(String awsAccessKey, String awsSecretKey, String awsRegion, String queueName) {
        /*
        To run this example, first create a queue using the AWS console. Take note of the name of the queue and the
        region where it was created.
        You will need an Access Key to access the queue. You can use your personal key in this example, but for
        production it's good practice to create a new key specific for the application and give only the permission to
        access the queue. The ACCESS_KEY_ID and SECRET_KEY are written in configuration file
        (e.g.: application.properties) or configuration service (e.g.: Spring Cloud Config) and read by AWS-SDK via
        system properties.
         */

        // Load configuration.
        System.setProperty("aws.accessKeyId", awsAccessKey);
        System.setProperty("aws.secretKey", awsSecretKey);

        // Create SQS client.
        AmazonSQS sqsClient = AmazonSQSClientBuilder
                .standard()
                .withCredentials(new SystemPropertiesCredentialsProvider())
                .withRegion(awsRegion)
                .build();
        log.info("SQS client created.");

        /*
        The SQS client needs to be created only once during application startup and saved in a singleton, context or
        alike, so this instance can be reused.
         */

        // Get queue URL.
        String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
        log.info("Queue found.");

        // Send a message.
        SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, "Test SQS send and receive.");
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);
        String sentMessageId = sendMessageResult.getMessageId();
        log.info("Message {} sent.", sentMessageId);

        /*
        By default, receiveMessage uses short-polling mode. In this mode, each request returns from zero to ten messages.
        If you didn't receive any message, that doesn't means there's no message in the queue. Because of the distributed
        nature of the queue, the server that responded your request may had not received a copy of the message yet. The
        next request will probably return the message.
        If there's no message, the call will not block. Instead, you will receive an empty list as response.
         */

        // Receive messages.
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> receivedMessages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
        for (Message message : receivedMessages) {

            // Handle message.
            String messageId = message.getMessageId();
            String messageBody = message.getBody();
            String receiptHandle = message.getReceiptHandle();
            log.info("Received message {} with text \"{}\".", messageId, messageBody);

            /*
            The message may come out-of-order and sometimes duplicated.
            You may use the messageId to check if this message was already handled and skip it in the second time.
             */

            // Delete message.
            DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
            sqsClient.deleteMessage(deleteMessageRequest);

            /*
            You must explicit delete the message. Otherwise, SQS will interpret that the server who got the message
            crashed before handling it and will return the same message again in a later call to receiveMessage.
            If the message should be received again in case of a rollback, delete it inside the try block.
            If the message should always be deleted, it's safer to delete it in a finally block.
             */
        }
    }

}
