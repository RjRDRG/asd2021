package protocols.membership.hyparview.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;
import protocols.membership.full.messages.SampleMessage;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HyParViewMessage extends ProtoMessage {

    public final static short MSG_ID = 101;
    private HyParViewMessageType type;
    private byte[] message;
    private static Host host;
    private int ttl;
    private volatile int size = -1;

    public HyParViewMessage(HyParViewMessageType type, int ttl, Host host) {
        this(type, new byte[]{}, ttl, host);
    }

    public HyParViewMessage(HyParViewMessageType type, byte[] message, int ttl, Host host) {
        super(MSG_ID);
        this.type = type;
        this.message = message;
        this.ttl = ttl;
        HyParViewMessage.host = host;
    }

    public byte[] getMessage() {
        return message;
    }

    public int getTtl() { return ttl; }

    public Host getHost() {return host; }

    @Override
    public String toString() {
        return "GenericHyParViewMessage{" +
                "host=" + host.getAddress() + ":" + host.getPort() +
                "type=" + type +
                "ttl=" + ttl +
                '}';
    }

    public static ISerializer<HyParViewMessage> serializer = new ISerializer<HyParViewMessage>() {
        @Override
        public void serialize(HyParViewMessage message, ByteBuf out) throws IOException {
            out.writeInt(message.type.ordinal());
            out.writeInt(message.ttl);
            Host.serializer.serialize(host, out);
        }

        @Override
        public HyParViewMessage deserialize(ByteBuf in) throws IOException {
            HyParViewMessageType type = HyParViewMessageType.values()[in.readInt()];
            int ttl = in.readInt();
            Host host = Host.serializer.deserialize(in);
            return new HyParViewMessage(type, ttl, host);
        }
    };

    public enum HyParViewMessageType {
        JOIN,
        FORWARD_JOIN,
        DISCONNECT,
        JOIN_BACK,
        NEIGHBOUR
    }
}
