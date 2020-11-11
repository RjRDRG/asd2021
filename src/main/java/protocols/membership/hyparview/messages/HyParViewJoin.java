package protocols.membership.hyparview.messages;

import network.data.Host;

public class HyParViewJoin extends HyParViewMessage {

    public static final short MSG_ID = 104;

    public HyParViewJoin(int ttl, Host host) {
        super(MSG_ID, ttl, host);
    }
}
