package protocols.membership.common.requests;

import babel.generic.ProtoRequest;

import java.util.UUID;

public class GetMembership extends ProtoRequest {

    public static final short REQUEST_ID = 101;

    private UUID requestID;

    public GetMembership(){
        super(REQUEST_ID);
    }

    public GetMembership(UUID id) {
        super(REQUEST_ID);
        this.requestID = id;
    }

    public UUID getIdentifier() {
        return requestID;
    }
}
