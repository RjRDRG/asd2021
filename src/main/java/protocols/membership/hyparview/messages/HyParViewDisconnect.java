package protocols.membership.hyparview.messages;

import network.data.Host;

public class HyParViewDisconnect extends HyParViewMessage {

    public static final short MSG_ID = 106;

    public HyParViewDisconnect(int ttl, Host host) {
        super(MSG_ID, ttl, host);
    }
}
