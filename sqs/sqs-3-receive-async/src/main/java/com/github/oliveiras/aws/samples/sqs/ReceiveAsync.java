package com.github.oliveiras.aws.samples.sqs;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.client.builder.ExecutorFactory;
import com.amazonaws.handlers.AsyncHandler;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.amazonaws.services.sqs.AmazonSQSAsyncClientBuilder;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient;
import com.amazonaws.services.sqs.buffered.QueueBufferConfig;
import com.amazonaws.services.sqs.model.*;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Example showing how to receive messages from SQS with a connection that stay open for a long period.
 *
 * @author Alex Oliveira
 */
public class ReceiveAsync {

    private static Logger log = LoggerFactory.getLogger(ReceiveAsync.class);

    private static AmazonSQSBufferedAsyncClient sqsClient;
    private static String queueUrl;

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
            receiveAsync(awsAccessKey, awsSecretKey, awsRegion, queueName, waitTime);
            log.info("Example completed.");

        } catch (ParseException | NumberFormatException e) {
            new HelpFormatter().printHelp("receive-async", options);
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
    private static void receiveAsync(String awsAccessKey, String awsSecretKey, String awsRegion, String queueName, int waitTime) throws InterruptedException {
        /*
        To run this example, use an existing queue.
        Send messages through AWS console and see them being printed here.
        Use CTRL+C to stop.
         */

        // Load configuration.
        System.setProperty("aws.accessKeyId", awsAccessKey);
        System.setProperty("aws.secretKey", awsSecretKey);

        // Create the ExecutorService to handle the messages.
        ExecutorService executorService = Executors.newFixedThreadPool(20);

        /*
        Since the messages are handled asynchronously, we can (and probably should) pass an executor service to the
        SQS client to make it call the AsyncHandler on the right threads.
        In a real application, you should use the ExecutorService from the framework/container.
        E.g.: In Java EE you would use `@Resource ManagedExecutorService executorService;`.
         */

        // Create SQS client.
        sqsClient = new AmazonSQSBufferedAsyncClient(AmazonSQSAsyncClientBuilder
                .standard()
                .withCredentials(new SystemPropertiesCredentialsProvider())
                .withRegion(awsRegion)
                .withExecutorFactory(() -> executorService)
                .build());
        log.info("SQS client created.");

        /*
        This client is async, which means that calls to `receiveMessages` will return immediately and the AsyncHandler
        will be invoked when a message arrives.
        Note that instead of passing an AsyncHandler, you can receive a Future. But please, don't do this just to call
        `get` and block your thread, this would totally dismiss the benefits of using an async client.
        This client is also buffered, which means that send and receive operations will not happen immediately, but
        instead will wait a little (200ms by default) to fill a buffer an then execute a batch operation. This increases
        throughput and reduce costs, but increase latency.
        The buffered client also use long polling by default.
         */

        // Get queue URL.
        queueUrl = sqsClient.getQueueUrl(queueName).getQueueUrl();
        log.info("Queue found.");

        // Receive messages and handle them async.
        log.info("Starting to listen for messages...");
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20);
        sqsClient.receiveMessageAsync(receiveMessageRequest, asyncHandler);

        /*
        The above call returns immediately, but the client is still waiting on it's own thread.
        When a message arrives or the connection times out, the handler is called.
        Note that each call to `receiveMessage` is one request to SQS, so you need to call it again inside the handler.
         */

        // This is here because if we let the `main` method returns, the program will exit.
//        executorService.awaitTermination(5, TimeUnit.MINUTES);
        Thread.sleep(300000);
    }

    /**
     * You should implements AsyncHandler interface to get the result of `receiveMessageAsync`.
     */
    private static AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult> asyncHandler = new AsyncHandler<ReceiveMessageRequest, ReceiveMessageResult>() {

        /**
         * This method is called when a message arrives or when the connection times out (in latter case, the list is empty).
         */
        public void onSuccess(ReceiveMessageRequest request, ReceiveMessageResult result) {

            // Receive messages.
            List<Message> receivedMessages = result.getMessages();
            log.info("Received {} messages", receivedMessages.size());
            for (Message message : receivedMessages) {

                /*
                Note that SQS client is calling this method on a thread from our ExecutorService.
                 */

                // Handle message.
                String messageId = message.getMessageId();
                String messageBody = message.getBody();
                String receiptHandle = message.getReceiptHandle();
                long threadId = Thread.currentThread().getId();
                log.info("Received message {} at thread {} with text \"{}\".", messageId, threadId, messageBody);

                // Delete message.
                DeleteMessageRequest deleteMessageRequest = new DeleteMessageRequest(queueUrl, receiptHandle);
                sqsClient.deleteMessage(deleteMessageRequest);
            }

            // Wait for new messages asynchronously again.
            ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl).withWaitTimeSeconds(20);
            sqsClient.receiveMessageAsync(receiveMessageRequest, this);

        }

        /**
         * I never saw this method being called.
         */
        public void onError(Exception e) {
            log.error("Ops!", e);
        }
    };

}
