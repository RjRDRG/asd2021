package protocols.membership.hyparview.messages;

import network.data.Host;

public class HyParViewJoinBack extends HyParViewMessage {

    public static final short MSG_ID = 107;

    public HyParViewJoinBack(int ttl, Host host) {
        super(MSG_ID, ttl, host);
    }
}
