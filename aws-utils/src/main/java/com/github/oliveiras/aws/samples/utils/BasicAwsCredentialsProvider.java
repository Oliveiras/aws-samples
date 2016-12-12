package com.github.oliveiras.aws.samples.utils;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;

/**
 * @author Alex Oliveira
 */
public class BasicAwsCredentialsProvider implements AWSCredentialsProvider {

    private final AWSCredentials credentials;

    public BasicAwsCredentialsProvider(AWSCredentials credentials) {
        this.credentials = credentials;
    }

    public AWSCredentials getCredentials() {
        return credentials;
    }

    public void refresh() {
    }
}
