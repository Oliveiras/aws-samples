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
 * Example showing how to receive messages from SQS with a connection that stay open for a long period.
 *
 * @author Alex Oliveira
 */
public class ReceiveLongPolling {

    private static Logger log = LoggerFactory.getLogger(ReceiveLongPolling.class);

    /**
     * Entry-point of the example.
     */
    public static void main(String[] args) {
        // Create command line options.
        Options options = new Options()
                .addOption(Option.builder("a").longOpt("access-key").hasArg().required().desc("AWS Access Key").build())
                .addOption(Option.builder("s").longOpt("secret-key").hasArg().required().desc("AWS Secret Key").build())
                .addOption(Option.builder("r").longOpt("aws-region").hasArg().required().desc("AWS region").build())
                .addOption(Option.builder("q").longOpt("queue-name").hasArg().required().desc("SQS queue name").build())
                .addOption(Option.builder("t").longOpt("wait-time").hasArgs().desc("Time to keep connection open, in seconds (max 20)").build());

        try {
            // Parse command line arguments.
            CommandLineParser parser = new DefaultParser();
            CommandLine commands = parser.parse(options, args);
            String awsAccessKey = commands.getOptionValue("a");
            String awsSecretKey = commands.getOptionValue("s");
            String awsRegion = commands.getOptionValue("r");
            String queueName = commands.getOptionValue("q");
            int waitTime = Integer.parseInt(commands.getOptionValue("t", "20"));
            log.info("Command line arguments parsed.");

            // Test SQS.
            receiveLongPolling(awsAccessKey, awsSecretKey, awsRegion, queueName, waitTime);
            log.info("Example completed.");

        } catch (ParseException | NumberFormatException e) {
            new HelpFormatter().printHelp("receive-long-polling", options);
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
    private static void receiveLongPolling(String awsAccessKey, String awsSecretKey, String awsRegion, String queueName, int waitTime) {
        /*
        To run this example, use an existing queue.
        Send messages through AWS console and see them being printed here.
        Use CTRL+C to stop.
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

        // Get queue URL.
        String queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
        log.info("Queue found.");

        /*
        Long polling will block this thread for up to `waitTime` seconds.
        When a message is sent to the queue, the method returns immediately with it. It may also get more than one message.
        If the request times out, the method just returns an empty list.
         */

        // Receive messages forever.
        log.info("Starting to listen for messages...");
        while (true) {
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                    .withWaitTimeSeconds(waitTime);
            List<Message> receivedMessages = sqsClient.receiveMessage(receiveMessageRequest).getMessages();
            log.info("Long-polling got {} messages.", receivedMessages.size());
            for (Message message : receivedMessages) {

                // Handle message.
                String messageId = message.getMessageId();
                String messageBody = message.getBody();
                String receiptHandle = message.getReceiptHandle();
                log.info("Received message {} with text \"{}\".", messageId, messageBody);

                // Delete message.
                DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
                sqsClient.deleteMessage(deleteMessageRequest);
            }
        }
    }

}
