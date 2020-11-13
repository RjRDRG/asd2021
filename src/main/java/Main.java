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
        BroadcastApp broadcastApp = new BroadcastApp(myself, props, PlumtreeBroadcast.PROTOCOL_ID);
        // Broadcast Protocol
        PlumtreeBroadcast broadcast = new PlumtreeBroadcast(props, myself);
        // Membership Protocol
        //SimpleFullMembership membership = new SimpleFullMembership(props, myself);
        SimpleFullMembership membership = new SimpleFullMembership(props, myself);

        //Register applications in babel
        babel.registerProtocol(broadcastApp);
        babel.registerProtocol(broadcast);
        babel.registerProtocol(membership);

        //Init the protocols. This should be done after creating all protocols, since there can be inter-protocol
        //communications in this step.
        broadcastApp.init(props);
        broadcast.init(props);
        membership.init(props);

        //Start babel and protocol threads
        babel.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> logger.info("Goodbye")));

    }

}
