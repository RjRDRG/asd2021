package protocols.membership.common.replies;

import babel.generic.ProtoReply;
import network.data.Host;
import protocols.membership.common.requests.GetMembership;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class GetMembershipReply extends ProtoReply {
    
    public static final short REPLY_ID = GetMembership.REQUEST_ID;

    private final UUID requestID;
    private final Set<Host> peers;

    public GetMembershipReply(UUID requestID, Set<Host> sample) {
        super(REPLY_ID);
        this.requestID = requestID;
        this.peers = new HashSet<>(sample);
    }

    public UUID getRequestID() {
        return requestID;
    }

    public Set<Host> getPeers() {
        return this.peers;
    }
    
}
