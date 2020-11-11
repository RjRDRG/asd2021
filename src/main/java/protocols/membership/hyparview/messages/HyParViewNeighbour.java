package protocols.membership.hyparview.messages;

import network.data.Host;

public class HyParViewNeighbour extends HyParViewMessage {

    public static final short MSG_ID = 108;

    public HyParViewNeighbour(int ttl, Host host) {
        super(MSG_ID, ttl, host);
    }
}
