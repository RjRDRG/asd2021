package protocols.membership.hyparview;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.exceptions.ProtocolDoesNotExist;
import babel.generic.ProtoMessage;
import babel.generic.ProtoRequest;
import babel.handlers.RequestHandler;
import channel.tcp.TCPChannel;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.replies.GetMembershipReply;
import protocols.membership.common.requests.GetMembership;
import protocols.membership.full.messages.SampleMessage;
import protocols.membership.hyparview.messages.*;

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

    private final int sampleTime; //param: timeout for samples
    private final int subsetSize; //param: maximum size of sample;

    private final Random rnd;

    private final int channelId; //Id of the created channel

    public HyParViewMembership(Properties props, Host self) throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.self = self;
        this.activeView = new HashSet<>();
        this.passiveView = new HashSet<>();

        this.rnd = new Random();

        //Get some configurations from the Properties object
        this.subsetSize = Integer.parseInt(props.getProperty("sample_size", "6"));
        this.sampleTime = Integer.parseInt(props.getProperty("sample_time", "2000")); //2 seconds
        this.maxActiveView = Integer.parseInt(props.getProperty("max_active_view", "5"));
        this.maxPassiveView = Integer.parseInt(props.getProperty("max_passive_view", "30"));
        this.arwl = Integer.parseInt(props.getProperty("arwl", "3"));
        this.prwl = Integer.parseInt(props.getProperty("prwl", "1"));

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
        registerRequestHandler(PROTOCOL_ID, this::registerRequestHandler);

        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, SampleMessage.MSG_ID, SampleMessage.serializer);

        /*---------------------- Register Message Handlers -------------------------- */
        registerMessageHandler(channelId, HyParViewJoin.MSG_ID, this::uponJoin, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewForwardJoin.MSG_ID, this::uponForwardJoin, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewDisconnect.MSG_ID, this::uponDisconnect, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewJoinBack.MSG_ID, this::uponJoinBack, this::uponMsgFail);
        registerMessageHandler(channelId, HyParViewNeighbour.MSG_ID, this::uponNeighbour, this::uponMsgFail);

        /*--------------------- Register Timer Handlers ----------------------------- */
        //registerTimerHandler(SampleTimer.TIMER_ID, this::uponSampleTimer);
        //registerTimerHandler(InfoTimer.TIMER_ID, this::uponInfoTime);

        /*-------------------- Register Channel Events ------------------------------- */
    }

    public void init(Properties props) {
        triggerNotification(new ChannelCreated(channelId));

        //If there is a contact node, attempt to establish connection
        if (props.containsKey("contact")) {
            try {
                String contact = props.getProperty("contact");
                String[] hostElems = contact.split(":");
                Host contactHost = new Host(InetAddress.getByName(hostElems[0]), Short.parseShort(hostElems[1]));
                //We add to the pending set until the connection is successful
                addNodeToActiveView(contactHost);
                openConnection(contactHost);
            } catch (Exception e) {
                logger.error("Invalid contact on configuration: '" + props.getProperty("contacts"));
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }

    /*--------------------------------- Requests ------------------------------------- */
    private void registerRequestHandler(ProtoRequest request, short sourceProto) {
        GetMembership req = (GetMembership) request;
        System.out.println("Req received: " + request.toString());

        GetMembershipReply reply = new GetMembershipReply(req.getIdentifier(), activeView);

        sendReply(reply, request.getId());
    }

    /*--------------------------------- Replies -------------------------------------- */


    /*--------------------------------- Messages ------------------------------------- */
    private void uponJoin(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponForwardJoin(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponDisconnect(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponJoinBack(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponNeighbour(SampleMessage msg, Host from, short sourceProto, int channelId) {

    }

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto,
                             Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }

    /*--------------------------------- Aux ------------------------------------------ */
    private void addNodeToPassiveView(Host newHost) {
        if (!newHost.equals(self) && !activeView.contains(newHost) && !passiveView.contains(newHost)) {
            if(passiveView.size() >= maxPassiveView) {
                passiveView.remove(getRandomHost(passiveView));
            }
            passiveView.add(newHost);
        }
    }

    private void addNodeToActiveView(Host newHost) {
        if(!newHost.equals(self) && !activeView.contains(newHost)) {
            if(activeView.size() >= maxActiveView) {
                dropRandomFromActiveView();
            }
            activeView.add(newHost);
        }
    }

    private void dropRandomFromActiveView() {
        Host rnd = getRandomHost(activeView);
        HyParViewMessage hm = new HyParViewDisconnect(0, self);
        sendMessage(hm,rnd);
        addNodeToPassiveView(rnd);
    }

    private Host getRandomHost(Set<Host> set) {
        return this.getRandomHost(set, null);
    }

    private Host getRandomHost(Set<Host> set, Host toBeRemoved){
        Set<Host> copy = new HashSet<>(set);
        if (toBeRemoved != null){
            copy.remove(toBeRemoved);
        }
        List<Host> array = new ArrayList<>(copy);
        Random rnd = new Random();
        if (array.isEmpty()){
            return null;
        }
        return array.get(rnd.nextInt(array.size()));
    }
}
