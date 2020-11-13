package protocols.broadcast.plumtree;

import babel.core.GenericProtocol;
import babel.exceptions.HandlerRegistrationException;
import babel.generic.ProtoMessage;
import network.data.Host;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import protocols.broadcast.common.BroadcastRequest;
import protocols.broadcast.common.DeliverNotification;
import protocols.broadcast.plumtree.messages.GossipMessage;
import protocols.broadcast.plumtree.messages.GraftMessage;
import protocols.broadcast.plumtree.messages.IHaveMessage;
import protocols.broadcast.plumtree.messages.PruneMessage;
import protocols.broadcast.plumtree.timers.GraftTimer;
import protocols.broadcast.plumtree.timers.LazyTimer;
import protocols.membership.common.notifications.ChannelCreated;
import protocols.membership.common.notifications.NeighbourDown;
import protocols.membership.common.notifications.NeighbourUp;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;

public class PlumtreeBroadcast extends GenericProtocol {
    private static final Logger logger = LogManager.getLogger(PlumtreeBroadcast.class);

    //Protocol information, to register in babel
    public static final String PROTOCOL_NAME = "Plumtree";
    public static final short PROTOCOL_ID = 245;

    private int channelId;
    
    private final Host myself; //My own address/port
    
    private final Set<Host> eagerPushPeers;
    
    private final Set<Host> lazyPushPeers; 
    private final Queue<IHaveMessage> lazyQueue; 
    
    private final long lazyPushBatchSizeInitial;
    private final long lazyPushRateInitial;
    private final long lazyPushDelayThreshold;
    private long lazyPushBatchSize;
    private long lazyPushRate;
    private Long lazyPushTimer;
    private long lazyPushDelayCounter;

    private final Map<UUID, GossipMessage> received; //Set of received messages 
    private final Map<UUID, Queue<IHaveMessage>> missing; //Set of missing messages
    
    private final Map<UUID, Long> graftTimers;
    private final int graftTimeout;
    
    private final long optimizationThreshold;
    
    //We can only start sending messages after the membership protocol informed us that the channel is ready
    private boolean channelReady;

    public PlumtreeBroadcast(Properties properties, Host myself) throws HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);
        this.myself = myself;
        
        eagerPushPeers = new HashSet<>();
        
        lazyPushPeers = new HashSet<>();
        lazyQueue = new LinkedList<>();
        
        lazyPushBatchSize = lazyPushBatchSizeInitial = Long.parseLong(properties.getProperty("lazy_push_batch_size", "10"));
        lazyPushRate = lazyPushRateInitial = Long.parseLong(properties.getProperty("lazy_push_rate", "2000"));
        lazyPushDelayThreshold = Long.parseLong(properties.getProperty("lazy_push_delay_threshold", "10")); 
        lazyPushTimer = null;
        lazyPushDelayCounter = 0;
        
        received = new HashMap<>();
        missing = new HashMap<>();
        
        graftTimers = new HashMap<>();
        graftTimeout = Integer.parseInt(properties.getProperty("graft_timeout", "2000")); //2 seconds
        
        optimizationThreshold = Long.parseLong(properties.getProperty("optimization_threshold", "2000")); //2 seconds
        
        channelReady = false;

        /*--------------------- Register Request Handlers ----------------------------- */
        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);

        /*--------------------- Register Notification Handlers ----------------------------- */
        subscribeNotification(NeighbourUp.NOTIFICATION_ID, this::uponNeighbourUp);
        subscribeNotification(NeighbourDown.NOTIFICATION_ID, this::uponNeighbourDown);
        subscribeNotification(ChannelCreated.NOTIFICATION_ID, this::uponChannelCreated);
        
        /*--------------------- Register Timer Handlers ----------------------------- */
        registerTimerHandler(GraftTimer.TIMER_ID, this::uponGraftTimer);
        registerTimerHandler(LazyTimer.TIMER_ID, this::uponLazyTimer);
    }

    @Override
    public void init(Properties props) {
        //Nothing to do here, we just wait for event from the membership or the application
    }

    //Upon receiving the channelId from the membership, register our own callbacks and serializers
    private void uponChannelCreated(ChannelCreated notification, short sourceProto) {
    	
        channelId = notification.getChannelId();
        
        // Allows this protocol to receive events from this channel.
        registerSharedChannel(channelId);
        /*---------------------- Register Message Serializers ---------------------- */
        registerMessageSerializer(channelId, GossipMessage.MSG_ID, GossipMessage.serializer);
        registerMessageSerializer(channelId, PruneMessage.MSG_ID, PruneMessage.serializer);
        registerMessageSerializer(channelId, IHaveMessage.MSG_ID, IHaveMessage.serializer);
        registerMessageSerializer(channelId, GraftMessage.MSG_ID, GraftMessage.serializer);
        
        /*---------------------- Register Message Handlers -------------------------- */
        try {
            registerMessageHandler(channelId, GossipMessage.MSG_ID, this::uponGossipMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        try {
            registerMessageHandler(channelId, PruneMessage.MSG_ID, this::uponPruneMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        try {
            registerMessageHandler(channelId, IHaveMessage.MSG_ID, this::uponIHaveMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        try {
            registerMessageHandler(channelId, GraftMessage.MSG_ID, this::uponGraftMessage, this::uponMsgFail);
        } catch (HandlerRegistrationException e) {
            logger.error("Error registering message handler: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
        
        //Now we can start sending messages
        channelReady = true;
    }

    /*--------------------------- Protocol Invariants ----------------------------------- */
    
    private boolean invPeers() {
    	Set<Host> intersection = new HashSet<>(eagerPushPeers);
    	intersection.retainAll(lazyPushPeers);
    	return intersection.isEmpty();
    }
    
    private boolean invSelf() {
    	return !eagerPushPeers.contains(myself) && !lazyPushPeers.contains(myself);
    }
    
    private boolean invMessages() {
    	Set<UUID> intersection = new HashSet<>(received.keySet());
    	intersection.retainAll(missing.keySet());
    	return intersection.isEmpty();
    }
    
    private boolean invTimers() {
    	return missing.keySet().equals(graftTimers.keySet());
    }
    
    private void assertInvariants() {
    	assert invPeers() : "peers invariant compromissed";
    	assert invSelf() : "self invariant compromissed";
    	assert invMessages() : "messages invariant compromissed";
    	assert invTimers() : "timers invariant compromissed";
    }
  
    /*--------------------------------- Requests ---------------------------------------- */
    private void uponBroadcastRequest(BroadcastRequest request, short sourceProto) {
        if (!channelReady) return;
        
        //Create the message object.
        GossipMessage gm = new GossipMessage(request.getMsgId(), request.getSender(), sourceProto, request.getMsg());

        uponGossipMessage(gm, myself, getProtoId(), channelId);
    }
    
    /*--------------------------------- Messages ---------------------------------------- */
    
    private void uponGossipMessage(GossipMessage m, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", m, from);
        
        UUID mid = m.getMid();
        
        //If we already received it once, do nothing (or we would end up with a nasty infinite loop)
        if (!received.containsKey(mid)) {
        	
            //Deliver the message to the application (even if it came from it)
            triggerNotification(new DeliverNotification(m.getMid(), m.getSource(), m.getContent()));

            received.put(mid, m);
            
            if(missing.containsKey(mid))
            	cancelTimer(graftTimers.remove(mid));
            
            eagerPushMessage(m, from, sourceProto, channelId);
            lazyPushMessage(m, from, sourceProto, channelId);
            
            if(lazyPushTimer == null) {
            	lazyPushTimer = setupTimer(new LazyTimer(), lazyPushRate);
            }
            
            if(!from.equals(myself)) {
            	eagerPushPeers.add(from);
            	lazyPushPeers.remove(from);
            	optimize(m, from, sourceProto, channelId);
            }
            
            missing.remove(mid);
        }
        else if(!from.equals(myself)) {
    		eagerPushPeers.remove(from);
    		lazyPushPeers.add(from);
    		
    		PruneMessage pm = new PruneMessage(myself);
        	sendMessage(pm, from);
        	
        	logger.info("Sent {} to {}", pm, from);
        }
        
        assertInvariants();
    }
    
    private void uponPruneMessage(PruneMessage m, Host from, short sourceProto, int channelId) {
    	logger.info("Received {} from {}", m, from);
    	
        eagerPushPeers.remove(from);
        lazyPushPeers.add(from);
        
        assertInvariants();
    }
    
    private void uponIHaveMessage(IHaveMessage m, Host from, short sourceProto, int channelId) {
        logger.info("Received {} from {}", m, from);
        
        UUID mid = m.getMid();
        
        if (!received.containsKey(mid)) {
        	
        	missing.putIfAbsent(m.getMid(), new LinkedList<>());
        	missing.get(mid).add(m);
        	
        	if(!graftTimers.containsKey(mid)) {
        		long timerId = setupTimer(new GraftTimer(mid), graftTimeout);
        		graftTimers.put(mid, timerId);
        	} 
        }
        
        assertInvariants();
    }
    
    private void uponGraftMessage(GraftMessage m, Host from, short sourceProto, int channelId) {
    	logger.info("Received {} from {}", m, from);
    	
    	UUID mid = m.getMid();
    	
        eagerPushPeers.add(from);
        lazyPushPeers.remove(from);
        GossipMessage gm = received.get(mid);
        if(m.requestGossipMessage() && gm != null) {
        	sendMessage(gm, from);
        	logger.info("Sent {} to {}", gm, from);
        }
        
        assertInvariants();
    }
    

    private void uponMsgFail(ProtoMessage msg, Host host, short destProto, Throwable throwable, int channelId) {
        //If a message fails to be sent, for whatever reason, log the message and the reason
        logger.error("Message {} to {} failed, reason: {}", msg, host, throwable);
    }
    
    /*-------------------------------------- Timers --------------------------------------------- */
    private void uponGraftTimer(GraftTimer timer, long timerId) {
        logger.info("Graft Timeout: {}", timer.getMid());
        
        UUID mid = timer.getMid();

        Queue<IHaveMessage> holders = missing.get(mid);

        if(holders != null && !holders.isEmpty()) {
            Host sender = holders.poll().getSender();

            eagerPushPeers.add(sender);
            lazyPushPeers.remove(sender);

            GraftMessage gm = new GraftMessage(mid, true, myself);
            sendMessage(gm, sender);

            logger.info("Sent {} to {}", gm, sender);

            if (!holders.isEmpty()) {
                graftTimers.put(mid, setupTimer(timer, graftTimeout * 2));
            }
        }
        assertInvariants();
    }
    
    private void uponLazyTimer(LazyTimer timer, long timerId) {
    	Set<IHaveMessage> sm = dispatchPolicy();
    	
    	if(sm == null) {
    		lazyPushDelayCounter++;
    		double denominator = Math.pow(lazyPushDelayCounter, 0.7);
    		lazyPushRate += lazyPushRateInitial/denominator;
    		lazyPushBatchSize += lazyPushBatchSizeInitial/denominator;
    		
    		lazyPushTimer = setupTimer(timer, lazyPushRate);
    	}
    	else {
    		lazyPushDelayCounter = Math.max(0, lazyPushDelayCounter-1);
    		lazyPushRate = lazyPushRateInitial;
    		lazyPushBatchSize = lazyPushBatchSizeInitial;
    		
    		for(IHaveMessage hm : sm) {
    			sendMessage(hm, hm.getTarget());
    			logger.info("Sent {} to {}", hm, hm.getTarget());	
    		}
    		
    		lazyPushTimer = setupTimer(timer, lazyPushRate);
    	}
    	
    	assertInvariants();
    }
    
    
    /*------------------------------------ Procedures ------------------------------------------- */
    
    private void eagerPushMessage(GossipMessage m, Host from, short sourceProto, int channelId) {
    	for(Host peer : eagerPushPeers) {
            if (!peer.equals(from)) {
                sendMessage(m, peer);
                logger.info("Sent {} to {}", m, peer);
            }
        }
    }
    
    private void lazyPushMessage(GossipMessage m, Host from, short sourceProto, int channelId) {
    	for(Host peer : lazyPushPeers) {
    		if (!peer.equals(from)) {
    			IHaveMessage hm = new IHaveMessage(m.getMid(), myself, peer);
    			lazyQueue.add(hm); 
    		}
		}
    }
    
    private Set<IHaveMessage> dispatchPolicy() {
    	if(lazyQueue.size() >= lazyPushBatchSize || lazyPushDelayCounter >= lazyPushDelayThreshold) {
    		long n = Math.min(lazyPushBatchSize, lazyQueue.size());
    		Set<IHaveMessage> peers = new HashSet<>();
    		for(long i=0; i<n; i++) { peers.add(lazyQueue.poll()); }
    		
    		return peers;
    	}
    	else {
    		return null;
    	}
    }
    
    private void optimize(GossipMessage m, Host from, short sourceProto, int channelId) {
    	if(missing.containsKey(m.getMid())) {
    		IHaveMessage hm = missing.get(m.getMid()).poll();
	    	Long currentTime = System.currentTimeMillis();
	    	
	    	if(hm != null && (currentTime - hm.getReceptionTime()) < optimizationThreshold) {
	    		Host target = hm.getSender();
	    		
	    		GraftMessage gm = new GraftMessage(m.getMid(), false, myself);
	    		sendMessage(gm, target);
	    		logger.info("Sent {} to {}", gm, target);
	    		
	    		PruneMessage pm = new PruneMessage(myself);
	    		sendMessage(pm, from);
	    		logger.info("Sent {} to {}", pm, from);
	    		
	    		eagerPushPeers.add(target);
	    		eagerPushPeers.remove(from);
	    		lazyPushPeers.add(from);
	    		lazyPushPeers.remove(target);
	    	}
    	}
    }

    /*--------------------------------- Notifications ---------------------------------------- */

    //When the membership protocol notifies of a new neighbour (or leaving one) simply update my list of neighbours.
    private void uponNeighbourUp(NeighbourUp notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
        	eagerPushPeers.add(h);
        	logger.debug("New neighbour: " + h);
        }
    }

    private void uponNeighbourDown(NeighbourDown notification, short sourceProto) {
        for(Host h: notification.getNeighbours()) {
        	eagerPushPeers.remove(h);
        	lazyPushPeers.remove(h);
        	
        	for(Queue<IHaveMessage> queue : missing.values()) {
        		queue.removeIf(p -> p.getSender().equals(h));
        	}
        	
	        logger.debug("Neighbour down: " + h);
	    }
    }
}
