import babel.core.Babel;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.apps.BroadcastApp;
import protocols.broadcast.flood.FloodBroadcast;
import protocols.broadcast.plumtree.PlumtreeBroadcast;
import protocols.membership.full.SimpleFullMembership;
import protocols.membership.hyparview.HyParViewMembership;
import utils.InterfaceToIp;

import java.net.InetAddress;
import java.util.Properties;


public class Main {

    //Sets the log4j (logging library) configuration file
    static {
        System.setProperty("log4j.configurationFile", "log4j2.xml");
    }

    //Creates the logger object
    private static final Logger logger = LogManager.getLogger(Main.class);

    //Default babel configuration file (can be overridden by the "-config" launch argument)
    private static final String DEFAULT_CONF = "config.properties";

    public static void main(String[] args) throws Exception {

        //Get the (singleton) babel instance
        Babel babel = Babel.getInstance();

        //Loads properties from the configuration file, and merges them with properties passed in the launch arguments
        Properties props = Babel.loadConfig(args, DEFAULT_CONF);

        //If you pass an interface name in the properties (either file or arguments), this wil get the IP of that interface
        //and create a property "address=ip" to be used later by the channels.
        InterfaceToIp.addInterfaceIp(props);

        //The Host object is an address/port pair that represents a network host. It is used extensively in babel
        //It implements equals and hashCode, and also includes a serializer that makes it easy to use in network messages
        Host myself =  new Host(InetAddress.getByName(props.getProperty("address")),
                Integer.parseInt(props.getProperty("port")));

        logger.info("Hello, I am {}", myself);

        // Application
        String broadcastProto = props.getProperty("broadcast_proto");
        BroadcastApp broadcastApp;
        if(broadcastProto.equals("flood")) {
            broadcastApp = new BroadcastApp(myself, props, FloodBroadcast.PROTOCOL_ID);
        } else if(broadcastProto.equals("plumtree")) {
            broadcastApp = new BroadcastApp(myself, props, PlumtreeBroadcast.PROTOCOL_ID);
        } else {
            logger.warn("Invalid broadcast protocol... Choosing default (Flood)");
            broadcastApp = new BroadcastApp(myself, props, FloodBroadcast.PROTOCOL_ID);
        }

        boolean isFlood = true;
        // Broadcast Protocol
        FloodBroadcast floodBroadcast = null;
        PlumtreeBroadcast plumBroadcast = null;
        if(broadcastProto.equals("flood")) {
            floodBroadcast = new FloodBroadcast(props, myself);
        } else if(broadcastProto.equals("plumtree")){
            plumBroadcast = new PlumtreeBroadcast(props, myself);
            isFlood = false;
        } else {
            floodBroadcast = new FloodBroadcast(props, myself);
        }

        // Membership Protocol
        boolean isFull = true;
        SimpleFullMembership fullMembership = null;
        HyParViewMembership hyParViewMembership = null;
        String membershipProto = props.getProperty("membership_proto");
        if(membershipProto.equals("full")) {
            fullMembership = new SimpleFullMembership(props, myself);
        } else if(membershipProto.equals("hyparview")) {
            hyParViewMembership = new HyParViewMembership(props, myself);
            isFull = false;
        } else {
            fullMembership = new SimpleFullMembership(props, myself);
            logger.warn("Invalid membership protocol... Choosing default (Full)");
        }

        //Register applications in babel
        babel.registerProtocol(broadcastApp);

        if(isFlood) {
            babel.registerProtocol(floodBroadcast);
        } else {
            babel.registerProtocol(plumBroadcast);
        }

        if(isFull) {
            babel.registerProtocol(fullMembership);
        } else {
            babel.registerProtocol(hyParViewMembership);
        }

        //Init the protocols. This should be done after creating all protocols, since there can be inter-protocol
        //communications in this step.
        broadcastApp.init(props);
        if(isFlood) {
            floodBroadcast.init(props);
        } else {
            plumBroadcast.init(props);
        }
        if(isFull) {
            fullMembership.init(props);
        } else {
            hyParViewMembership.init(props);
        }

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));

    }

}
