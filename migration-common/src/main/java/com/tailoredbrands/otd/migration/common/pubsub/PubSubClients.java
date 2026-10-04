package com.tailoredbrands.otd.migration.common.pubsub;

import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.TopicName;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Publisher / Subscriber / admin client factory that honours {@code PUBSUB_EMULATOR_HOST}
 * (plaintext gRPC channel + {@link NoCredentialsProvider}) as required by CONVENTIONS.md.
 *
 * <p>Instances are cheap to create; the emulator channel (if any) is shared and closed by
 * {@link #close()}.</p>
 */
public class PubSubClients implements AutoCloseable {

    public static final long DEFAULT_FLOW_CONTROL_MESSAGES = 100L;

    private final PubSubSettings settings;
    private final ManagedChannel emulatorChannel;
    private final TransportChannelProvider channelProvider;
    private final CredentialsProvider credentialsProvider;

    public PubSubClients(PubSubSettings settings) {
        this.settings = settings;
        if (settings.usesEmulator()) {
            this.emulatorChannel = ManagedChannelBuilder.forTarget(settings.emulatorHost()).usePlaintext().build();
            this.channelProvider = FixedTransportChannelProvider.create(GrpcTransportChannel.create(emulatorChannel));
            this.credentialsProvider = NoCredentialsProvider.create();
        } else {
            this.emulatorChannel = null;
            this.channelProvider = null;
            this.credentialsProvider = null;
        }
    }

    public PubSubSettings settings() {
        return settings;
    }

    public String project() {
        return settings.project();
    }

    public TopicName topicName(String topic) {
        return TopicName.of(settings.project(), topic);
    }

    public ProjectSubscriptionName subscriptionName(String subscription) {
        return ProjectSubscriptionName.of(settings.project(), subscription);
    }

    /**
     * Publisher with message ordering enabled (ordering key = storeId for {@code orders-v1}).
     * Callers must call {@link Publisher#resumePublish(String)} after a failed ordered publish.
     */
    public Publisher publisher(String topic, boolean enableOrdering) throws IOException {
        Publisher.Builder builder = Publisher.newBuilder(topicName(topic))
                .setEnableMessageOrdering(enableOrdering);
        if (channelProvider != null) {
            builder.setChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
        }
        return builder.build();
    }

    /**
     * Streaming-pull subscriber: one pull stream (keeps ordering), flow control of
     * {@value #DEFAULT_FLOW_CONTROL_MESSAGES} outstanding messages.
     */
    public Subscriber subscriber(String subscription, MessageReceiver receiver) {
        return subscriber(subscription, receiver, DEFAULT_FLOW_CONTROL_MESSAGES);
    }

    public Subscriber subscriber(String subscription, MessageReceiver receiver, long maxOutstandingMessages) {
        Subscriber.Builder builder = Subscriber.newBuilder(subscriptionName(subscription), receiver)
                .setParallelPullCount(1)
                .setFlowControlSettings(FlowControlSettings.newBuilder()
                        .setMaxOutstandingElementCount(maxOutstandingMessages)
                        .build());
        if (channelProvider != null) {
            builder.setChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
        }
        return builder.build();
    }

    /** Synchronous pull stub (used by tests and the reconciler's spot checks). */
    public SubscriberStub subscriberStub() throws IOException {
        SubscriberStubSettings.Builder builder = SubscriberStubSettings.newBuilder();
        if (channelProvider != null) {
            builder.setTransportChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
        }
        return GrpcSubscriberStub.create(builder.build());
    }

    public TopicAdminClient topicAdminClient() throws IOException {
        TopicAdminSettings.Builder builder = TopicAdminSettings.newBuilder();
        if (channelProvider != null) {
            builder.setTransportChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
        }
        return TopicAdminClient.create(builder.build());
    }

    public SubscriptionAdminClient subscriptionAdminClient() throws IOException {
        SubscriptionAdminSettings.Builder builder = SubscriptionAdminSettings.newBuilder();
        if (channelProvider != null) {
            builder.setTransportChannelProvider(channelProvider).setCredentialsProvider(credentialsProvider);
        }
        return SubscriptionAdminClient.create(builder.build());
    }

    /** Shuts a publisher down gracefully, flushing pending batches. */
    public static void shutdownQuietly(Publisher publisher) {
        if (publisher == null) {
            return;
        }
        try {
            publisher.shutdown();
            publisher.awaitTermination(30, TimeUnit.SECONDS);
        } catch (Exception ignored) {
            // best effort
        }
    }

    @Override
    public void close() {
        if (emulatorChannel != null) {
            emulatorChannel.shutdownNow();
        }
    }
}
