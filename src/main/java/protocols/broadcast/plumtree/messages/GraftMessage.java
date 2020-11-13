package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class GraftMessage extends ProtoMessage {
    public static final short MSG_ID = 202;

    private final UUID mid;
    private final boolean requestGossipMessage;
    private final Host sender;

    @Override
    public String toString() {
        return "GraftMessage{" +
                "mid=" + mid +
                '}';
    }

    public GraftMessage(UUID mid, boolean requestGossipMessage, Host sender) {
		super(MSG_ID);
        this.mid = mid;
        this.requestGossipMessage = requestGossipMessage;
        this.sender = sender;
    }

    public Host getSender() {
        return sender;
    }

    public UUID getMid() {
        return mid;
    }
    
    public boolean requestGossipMessage() {
    	return requestGossipMessage;
    }

    public static ISerializer<GraftMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GraftMessage graftMessage, ByteBuf out) throws IOException {
            out.writeLong(graftMessage.mid.getMostSignificantBits());
            out.writeLong(graftMessage.mid.getLeastSignificantBits());
            out.writeBoolean(graftMessage.requestGossipMessage);
            Host.serializer.serialize(graftMessage.sender, out);
        }

        @Override
        public GraftMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            boolean requestMessageWithId = in.readBoolean();
            Host sender = Host.serializer.deserialize(in);

            return new GraftMessage(mid, requestMessageWithId, sender);
        }
    };
}
