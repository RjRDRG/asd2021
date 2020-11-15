package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class PruneMessage extends ProtoMessage {
    public static final short MSG_ID = 204;

    private final UUID mid;
    private final Host sender;

    @Override
    public String toString() {
        return "PruneMessage{" +
                "mid=" + mid +
                '}';
    }

    public PruneMessage(UUID mid, Host sender) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
    }

    public UUID getMid() {
        return mid;
    }

    public Host getSender() {
        return sender;
    }

    public static ISerializer<PruneMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PruneMessage pruneMessage, ByteBuf out) throws IOException {
            out.writeLong(pruneMessage.mid.getMostSignificantBits());
            out.writeLong(pruneMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(pruneMessage.sender, out);
        }

        @Override
        public PruneMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);

            return new PruneMessage(mid, sender);
        }
    };
}
