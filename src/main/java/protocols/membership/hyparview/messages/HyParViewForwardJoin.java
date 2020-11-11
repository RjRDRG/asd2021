package protocols.membership.hyparview.messages;

import network.data.Host;

public class HyParViewForwardJoin extends HyParViewMessage {

    public static final short MSG_ID = 105;

    public HyParViewForwardJoin(int ttl, Host host) {
        super(MSG_ID, ttl, host);
    }
}
