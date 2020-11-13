package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class GossipMessage extends ProtoMessage {
    public static final short MSG_ID = 201;

    private final UUID mid;
    private final Host source;

    private final short toDeliver;
    private final byte[] content;

    @Override
    public String toString() {
        return "GossipMessage{" +
                "mid=" + mid +
                '}';
    }

    public GossipMessage(UUID mid, Host source, short toDeliver, byte[] content) {
        super(MSG_ID);
        this.mid = mid;
        this.source = source;
        this.toDeliver = toDeliver;
        this.content = content;
    }

    public Host getSource() {
        return source;
    }

    public UUID getMid() {
        return mid;
    }

    public short getToDeliver() {
        return toDeliver;
    }

    public byte[] getContent() {
        return content;
    }

    public static ISerializer<GossipMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GossipMessage gossipMessage, ByteBuf out) throws IOException {
            out.writeLong(gossipMessage.mid.getMostSignificantBits());
            out.writeLong(gossipMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(gossipMessage.source, out);
            out.writeShort(gossipMessage.toDeliver);
            out.writeInt(gossipMessage.content.length);
            if (gossipMessage.content.length > 0) {
                out.writeBytes(gossipMessage.content);
            }
        }

        @Override
        public GossipMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            short toDeliver = in.readShort();
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            return new GossipMessage(mid, sender, toDeliver, content);
        }
    };
}
