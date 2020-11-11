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

    public final static short DEFAULT_MSG_ID = 101;
    public final short msg_id;
    protected final byte[] message;
    protected static Host host;
    protected final int ttl;
    private final int size = -1;

    public HyParViewMessage(int ttl, Host host) {
        this(DEFAULT_MSG_ID, new byte[]{}, ttl, host);
    }

    public HyParViewMessage(short msg_id, int ttl, Host host) {
        this(msg_id, new byte[]{}, ttl, host);
    }

    public HyParViewMessage(short msg_id, byte[] message, int ttl, Host host) {
        super(msg_id);
        this.msg_id = msg_id;
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
                "type=" + msg_id +
                "ttl=" + ttl +
                '}';
    }

    public static ISerializer<HyParViewMessage> serializer = new ISerializer<HyParViewMessage>() {
        @Override
        public void serialize(HyParViewMessage message, ByteBuf out) throws IOException {
            out.writeInt(message.ttl);
            Host.serializer.serialize(host, out);
        }

        @Override
        public HyParViewMessage deserialize(ByteBuf in) throws IOException {
            int ttl = in.readInt();
            Host host = Host.serializer.deserialize(in);
            return new HyParViewMessage(ttl, host);
        }
    };
}
