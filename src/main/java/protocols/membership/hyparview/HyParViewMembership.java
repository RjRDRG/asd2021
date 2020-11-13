package protocols.membership.hyparview;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.ProtocolDoesNotExist;
import babel.generic.ProtoMessage;
import babel.generic.ProtoReply;
import babel.generic.ProtoRequest;
import babel.handlers.RequestHandler;
import channel.tcp.TCPChannel;
import channel.tcp.events.*;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;
import protocols.membership.common.replies.GetMembershipReply;
import protocols.membership.common.requests.GetMembership;
import protocols.membership.full.messages.SampleMessage;
import protocols.membership.full.timers.SampleTimer;
import protocols.membership.hyparview.messages.*;
import protocols.membership.hyparview.timers.HyParViewTimer;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

public class HyParViewMembership extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(HyParViewMembership.class);

    //Protocol information, to register in babel
    public final static short PROTOCOL_ID = 100;
    public final static String PROTOCOL_NAME = "HyParViewMembership";

    private final int maxActiveView;
    private final int maxPassiveView;
    private final int arwl; //Active Random Walk Length
    private final int prwl; //Passive Random Walk Length

    private final Host self;     //My own address/port
    private final Set<Host> activeView; //Peers I am connected to
    private final Set<Host> passiveView; //Peers I am trying to connect to

    private final int ka; //param: timeout for samples
    private final int kp; //param: maximum size of sample;
    private final int shuffleTime; //param: maximum size of sample;

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public HyParViewMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();

        this.rnd = new Random();

        //Get some configurations from the Properties object
        this.shuffleTime = Integer.parseInt(props.getProperty("shuffle_time", "13000"));
        this.ka = Integer.parseInt(props.getProperty("active_view_subset_size", "2"));
        this.kp = Integer.parseInt(props.getProperty("passive_view_subset_size", "2"));
        this.maxActiveView = Integer.parseInt(props.getProperty("max_active_view", "5"));
        this.maxPassiveView = Integer.parseInt(props.getProperty("max_passive_view", "30"));
        this.arwl = Integer.parseInt(props.getProperty("arwl", "3"));
        this.prwl = Integer.parseInt(props.getProperty("prwl", "2"));

        String cMetricsInterval = props.getProperty("channel_metrics_interval", "10000"); //10 seconds

        //Create a properties object to setup channel-specific properties. See the channel description for more details.
        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, props.getProperty("address")); //The address to bind to
        channelProps.setProperty(TCPChannel.PORT_KEY, props.getProperty("port")); //The port to bind to
        channelProps.setProperty(TCPChannel.METRICS_INTERVAL_KEY, cMetricsInterval); //The interval to receive channel metrics
        channelProps.setProperty(TCPChannel.HEARTBEAT_INTERVAL_KEY, "1000"); //Heartbeats interval for established connections
        channelProps.setProperty(TCPChannel.HEARTBEAT_TOLERANCE_KEY, "3000"); //Time passed without heartbeats until closing a connection
        channelProps.setProperty(TCPChannel.CONNECT_TIMEOUT_KEY, "1000"); //TCP connect timeout
        channelId = createChannel(TCPChannel.NAME, channelProps); //Create the channel with the given properties

        /*---------------------- Register Request Handlers ------------------------- */
        //registerRequestHandler(PROTOCOL_ID, this::requestHandler);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, HyParViewJoin.MSG_ID, HyParViewMessage.serializer);
        registerMessageSerializer(channelId, HyParViewForwardJoin.MSG_ID, HyParViewMessage.serializer);
        registerMessageSerializer(channelId, HyParViewDisconnect.MSG_ID, HyParViewMessage.serializer);
        registerMessageSerializer(channelId, HyParViewJoinBack.MSG_ID, HyParViewMessage.serializer);
        registerMessageSerializer(channelId, HyParViewNeighbour.MSG_ID, HyParViewMessage.serializer);
        //registerMessageSerializer(channelId, HyParViewShuffle.MSG_ID, HyParViewShuffle.serializer);
        //registerMessageSerializer(channelId, HyParViewShuffleReply.MSG_ID, HyParViewShuffleReply.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, HyParViewJoin.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewForwardJoin.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewDisconnect.MSG_ID, this::uponDisconnect, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewJoinBack.MSG_ID, this::uponJoinBack, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewNeighbour.MSG_ID, this::uponNeighbour, this::uponMsgFail);
        //registerMessageHandler(channelId, HyParViewShuffle.MSG_ID, this::uponShuffle, this::uponMsgFail);
        //registerMessageHandler(channelId, HyParViewShuffleReply.MSG_ID, this::uponShuffleReply, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        //registerTimerHandler(HyParViewTimer.TIMER_ID, this::uponPassiveViewTimer);

        /*-------------------- Register Channel Events ------------------------------- */
        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
        registerChannelEventHandler(channelId, ChannelMetrics.EVENT_ID, this::uponChannelMetrics);
    }


    public void init(Properties props) {

        //Inform the dissemination protocol about the channel we created in the constructor
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                openConnection(contactHost);
                sendMessage(new HyParViewJoin(0, self), contactHost);
                addNodeToActiveView(contactHost);
                logger.trace("{} moved to Active View", contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }

        //Setup the timer used to send shuffles
        //setupPeriodicTimer(new HyParViewTimer(), this.shuffleTime, this.shuffleTime);
    }

    /*--------------------------------- Messages ------------------------------------- */
    private void uponJoin(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received JOIN from {}", from);
        openConnection(from);
        HyParViewMessage forwardJoin = new HyParViewForwardJoin(arwl, from);
        for(Host h: activeView) {
            if(h != from) {
                logger.trace("Sending FORWARD_JOIN from {} to {}", from, h);
                sendMessage(forwardJoin, h);
            }
        }
    }

    private void uponForwardJoin(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received FORWARD_JOIN from {}", from);

        int ttl = msg.getTtl();
        Host newNode = msg.getHost();

        if (activeView.size() == 1 || ttl == 0) {
            openConnection(newNode);
            logger.trace("Sending JOIN_BACK to {}", from);
            sendMessage(new HyParViewJoinBack(0, self), newNode);
        } else {
            if (ttl == prwl) {
                logger.trace("{} added to Passive View", from);
                addNodeToPassiveView(newNode);
            }
            Host random = getRandomNode(activeView, from);
            if (random != null) {
                logger.trace("Redirected FORWARD_JOIN to {}", random);
                HyParViewMessage forwardJoin = new HyParViewForwardJoin(ttl - 1, newNode);
                sendMessage(forwardJoin, random);
            }
        }
    }

    private void uponDisconnect(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received DISCONNECT from {}", from);
        if(activeView.remove(from)) {
            closeConnection(from);
            logger.debug("Disconnected from {}", from);
            addNodeToPassiveView(from);
        }
    }

    private void uponJoinBack(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received JOIN_BACK from {}", from);
        openConnection(from);
    }

    private void uponNeighbour(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received NEIGHBOUR from {}", from);
        if(msg.getTtl() == 1 || activeView.size() < maxActiveView) {
            openConnection(from);
        }
    }

    private void uponShuffle(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received SHUFFLE from {}", from);
        HyParViewShuffle shuffleMsg = (HyParViewShuffle) msg;
        int ttl = shuffleMsg.getTtl()-1;

        if(ttl > 0 && activeView.size() > 1) {
            Host randomNode = getRandomNode(activeView, from);
            HyParViewMessage forwardMsg = new HyParViewShuffle(ttl, shuffleMsg.getHost(), shuffleMsg.getNodesSubset());
            sendMessage(forwardMsg, randomNode);
        } else {
            int shuffleSize = shuffleMsg.getNNodes();
            Set<Host> replyPassiveView = getRandomSubsetExcluding(passiveView, shuffleSize, from);
            logger.trace("Sending SHUFFLE_REPLY to {}", from);
            openConnection(from);
            sendMessage(new HyParViewShuffleReply(1, self, replyPassiveView, shuffleMsg.getNodesSubset()), from);
            closeConnection(from);
            addNodesToPassiveViewAfterShuffle(shuffleMsg.getNodesSubset(), replyPassiveView);
        }
    }

    private void uponShuffleReply(HyParViewMessage msg, Host from, short sourceProto, int channelId) {
        logger.trace("Received SHUFFLE_REPLY from {}", from);
        HyParViewShuffleReply shuffleReplyMsg = (HyParViewShuffleReply) msg;
        addNodesToPassiveViewAfterShuffle(shuffleReplyMsg.getNodesSubset(), shuffleReplyMsg.getOGNodesSubset());
    }

    private void uponMsgFail(HyParViewMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Timers ---------------------------------------- */
    private void uponPassiveViewTimer(HyParViewTimer timer, long timerId) {
        if(activeView.size() > 1) {
            Host randomNode = getRandomNode(activeView);
            logger.trace("Sending SHUFFLE to {}", randomNode);
            Set<Host> subset = getRandomSubsetExcluding(activeView, ka, randomNode);
            subset.addAll(getRandomSubset(passiveView, kp));
            HyParViewMessage shuffle = new HyParViewShuffle(arwl, self, subset);

            sendMessage(shuffle, randomNode);
        }
    }

    /* --------------------------------- TCPChannel Events ---------------------------- */

    //If a connection is successfully established, this event is triggered. In this protocol, we want to add the
    //respective peer to the membership, and inform the Dissemination protocol via a notification.
    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is up", peer);
        passiveView.remove(peer);
        if (addNodeToActiveView(peer)) {
            logger.trace("{} moved to Active View", peer);
            triggerNotification(new NeighbourUp(peer));
        }
    }

    //If an established connection is disconnected, remove the peer from the membership and inform the Dissemination
    //protocol. Alternatively, we could do smarter things like retrying the connection X times.
    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.debug("Connection to {} is down cause {}", peer, event.getCause());
        if(activeView.remove(peer) && passiveView.size() > 0) {
            triggerNotification(new NeighbourDown(peer));
            openConnection(getRandomNode(passiveView));
        }
    }

    //If a connection fails to be established, this event is triggered. In this protocol, we simply remove from the
    //pending set. Note that this event is only triggered while attempting a connection, not after connection.
    //Thus the peer will be in the pending set, and not in the membership (unless something is very wrong with our code)
    private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> event, int channelId) {
        logger.debug("Connection to {} failed cause: {}", event.getNode(), event.getCause());
        if(passiveView.remove(event.getNode())) {
            logger.trace("{} removed from Passive View", event.getNode());
        }
        openConnection(getRandomNode(passiveView));
        logger.trace("Attempting node {}", event.getNode());
    }

    //If someone established a connection to me, this event is triggered. In this protocol we do nothing with this event.
    //If we want to add the peer to the membership, we will establish our own outgoing connection.
    // (not the smartest protocol, but its simple)
    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.trace("Connection from {} is up", event.getNode());
    }

    //A connection someone established to me is disconnected.
    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        Host peer = event.getNode();
        logger.trace("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
        if(activeView.remove(peer) && passiveView.size() > 0) {
            openConnection(getRandomNode(passiveView));
        }
    }

    /* --------------------------------- Metrics ---------------------------- */

    //If we passed a value >0 in the METRICS_INTERVAL_KEY property of the channel, this event will be triggered
    //periodically by the channel. This is NOT a protocol timer, but a channel event.
    //Again, we are just showing some of the information you can get from the channel, and use how you see fit.
    //"getInConnections" and "getOutConnections" returns the currently established connection to/from me.
    //"getOldInConnections" and "getOldOutConnections" returns connections that have already been closed.
    private void uponChannelMetrics(ChannelMetrics event, int channelId) {
        StringBuilder sb = new StringBuilder("Channel Metrics:\n");
        sb.append("In channels:\n");
        event.getInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldInConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.append("Out channels:\n");
        event.getOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        event.getOldOutConnections().forEach(c -> sb.append(String.format("\t%s: msgOut=%s (%s) msgIn=%s (%s) (old)\n",
                c.getPeer(), c.getSentAppMessages(), c.getSentAppBytes(), c.getReceivedAppMessages(),
                c.getReceivedAppBytes())));
        sb.setLength(sb.length() - 1);
        logger.info(sb);
    }

    /*--------------------------------- Aux ------------------------------------------ */
    private void addNodesToPassiveViewAfterShuffle(Set<Host> hostSet, Set<Host> priority) {
        Set<Host> copy = new HashSet<>(hostSet);
        Set<Host> removeFirst = new HashSet<>(priority);
        copy.remove(self);
        for(Host h: hostSet) {
            if(activeView.contains(h) || passiveView.contains(h)) {
                copy.remove(h);
            }
        }
        for(Host h: copy) {
            if(passiveView.size() >= maxPassiveView) {
                Host temp;
                if(removeFirst.size() > 0) {
                    temp = getRandomNode(removeFirst);
                    removeFirst.remove(temp);
                } else {
                    temp = getRandomNode(passiveView);
                }
                passiveView.remove(temp);
            }
            passiveView.add(h);
        }
    }

    private boolean addNodeToPassiveView(Host newHost) {
        if (!newHost.equals(self) && !activeView.contains(newHost) && !passiveView.contains(newHost)) {
            if(passiveView.size() >= maxPassiveView) {
                passiveView.remove(getRandomNode(passiveView));
            }
            return passiveView.add(newHost);
        }
        return false;
    }

    private boolean addNodeToActiveView(Host newHost) {
        if(!newHost.equals(self) && !activeView.contains(newHost)) {
            if(activeView.size() >= maxActiveView) {
                dropRandomFromActiveView();
            }
            return activeView.add(newHost);
        }
        return false;
    }

    private void dropRandomFromActiveView() {
        Host randomNode = getRandomNode(activeView);
        HyParViewMessage disconnect = new HyParViewDisconnect(0, self);
        logger.trace("Sending DISCONNECT to {}", randomNode);
        sendMessage(disconnect, randomNode);
        closeConnection(randomNode);
        logger.debug("Disconnected from {}", randomNode);
        addNodeToPassiveView(randomNode);
    }

    private Host getRandomNode(Set<Host> set) {
        return this.getRandomNode(set, null);
    }

    private Host getRandomNode(Set<Host> set, Host toBeRemoved){
        Set<Host> copy = new HashSet<>(set);
        if (toBeRemoved != null){
            copy.remove(toBeRemoved);
        }
        int idx = rnd.nextInt(copy.size());
        int i = 0;
        for (Host h : copy) {
            if (i == idx)
                return h;
            i++;
        }
        return null;
    }

    //Gets a random subset from the set of peers
    private static Set<Host> getRandomSubsetExcluding(Set<Host> hostSet, int sampleSize, Host exclude) {
        List<Host> list = new LinkedList<>(hostSet);
        list.remove(exclude);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }

    //Gets a random subset from the set of peers
    private static Set<Host> getRandomSubset(Set<Host> hostSet, int sampleSize) {
        List<Host> list = new LinkedList<>(hostSet);
        Collections.shuffle(list);
        return new HashSet<>(list.subList(0, Math.min(sampleSize, list.size())));
    }
}
