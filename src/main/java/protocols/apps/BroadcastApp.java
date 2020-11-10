package protocols.apps;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.timers.BroadcastTimer;
import protocols.apps.timers.ExitTimer;
import protocols.apps.timers.StartTimer;
import protocols.apps.timers.StopTimer;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class BroadcastApp extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(BroadcastApp.class);

    //Protocol information, to register in babel
    public static final String PROTO_NAME = "BroadcastApp";
    public static final short PROTO_ID = 300;

    private final short broadcastProtoId;

    //Size of the payload of each message (in bytes)
    private final int payloadSize;
    //Time to wait until starting sending messages
    private final int prepareTime;
    //Time to run before shutting down
    private final int runTime;
    //Time to wait until starting sending messages
    private final int cooldownTime;
    //Interval between each broadcast
    private final int broadcastInterval;

    private final Host self;

    private long broadCastTimer;

    public BroadcastApp(Host self, Properties properties, short broadcastProtoId) throws HandlerRegistrationException {
        super(PROTO_NAME, PROTO_ID);
        this.broadcastProtoId = broadcastProtoId;
        this.self = self;

        //Read configurations
        this.payloadSize = Integer.parseInt(properties.getProperty("payload_size"));
        this.prepareTime = Integer.parseInt(properties.getProperty("prepare_time")); //in seconds
        this.cooldownTime = Integer.parseInt(properties.getProperty("cooldown_time")); //in seconds
        this.runTime = Integer.parseInt(properties.getProperty("run_time")); //in seconds
        this.broadcastInterval = Integer.parseInt(properties.getProperty("broadcast_interval")); //in milliseconds

        //Setup handlers
        subscribeNotification(DeliverNotification.NOTIFICATION_ID, this::uponDeliver);
        registerTimerHandler(BroadcastTimer.TIMER_ID, this::uponBroadcastTimer);
        registerTimerHandler(StartTimer.TIMER_ID, this::uponStartTimer);
        registerTimerHandler(StopTimer.TIMER_ID, this::uponStopTimer);
        registerTimerHandler(ExitTimer.TIMER_ID, this::uponExitTimer);
    }

    @Override
    public void init(Properties props) {
        //Wait prepareTime seconds before starting
        logger.info("Waiting...");
        setupTimer(new StartTimer(), prepareTime * 1000);
    }

    private void uponStartTimer(StartTimer startTimer, long timerId) {
        logger.info("Starting");
        //Start broadcasting periodically
        broadCastTimer = setupPeriodicTimer(new BroadcastTimer(), 0, broadcastInterval);
        //And setup the stop timer
        setupTimer(new StopTimer(), runTime * 1000);
    }

    private void uponBroadcastTimer(BroadcastTimer broadcastTimer, long timerId) {
        //Upon triggering the broadcast timer, create a new message
        String toSend = randomCapitalLetters(Math.max(0, payloadSize));
        //ASCII encodes each character as 1 byte
        byte[] payload = toSend.getBytes(StandardCharsets.US_ASCII);

        BroadcastRequest request = new BroadcastRequest(UUID.randomUUID(), self, payload);
        logger.info("Sending: {} - {} ({})", request.getMsgId(), toSend, payload.length);
        //And send it to the dissemination protocol
        sendRequest(request, broadcastProtoId);
    }

    private void uponDeliver(DeliverNotification reply, short sourceProto) {
        //Upon receiving a message, simply print it
        logger.info("Received {} - {} ({}) from {}", reply.getMsgId(),
                new String(reply.getMsg(), StandardCharsets.US_ASCII), reply.getMsg().length, reply.getSender());
    }

    private void uponStopTimer(StopTimer stopTimer, long timerId) {
        logger.info("Stopping broadcasts");
        this.cancelTimer(broadCastTimer);
        setupTimer(new ExitTimer(), cooldownTime * 1000);
    }
    private void uponExitTimer(ExitTimer exitTimer, long timerId) {
        logger.info("Exiting...");
        System.exit(0);
    }

    public static String randomCapitalLetters(int length) {
        int leftLimit = 65; // letter 'A'
        int rightLimit = 90; // letter 'Z'
        Random random = new Random();
        return random.ints(leftLimit, rightLimit + 1).limit(length)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
    }
}
