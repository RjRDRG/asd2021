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
    protected final byte[] content;
    protected Host host;
    protected final int ttl;

    public HyParViewMessage(int ttl, Host host) {
        this(DEFAULT_MSG_ID, new byte[]{}, ttl, host);
    }

    public HyParViewMessage(short msg_id, int ttl, Host host) {
        this(msg_id, new byte[]{}, ttl, host);
    }

    public HyParViewMessage(short msg_id, byte[] content, int ttl, Host host) {
        super(msg_id);
        this.msg_id = msg_id;
        this.content = content;
        this.ttl = ttl;
        this.host = host;
    }

    public byte[] getContent() {
        return content;
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
            out.writeShort(message.msg_id);
            Host.serializer.serialize(message.host, out);
            out.writeInt(message.content.length);
            if (message.content.length > 0) {
                out.writeBytes(message.content);
            }
        }

        @Override
        public HyParViewMessage deserialize(ByteBuf in) throws IOException {
            int ttl = in.readInt();
            short msg_id = in.readShort();
            Host host = Host.serializer.deserialize(in);
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);
            return new HyParViewMessage(msg_id, content, ttl, host);
        }
    };
}
