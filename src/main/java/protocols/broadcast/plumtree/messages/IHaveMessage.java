package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.UUID;

public class IHaveMessage extends ProtoMessage {
    public static final short MSG_ID = 203;

    private final UUID mid;
    private final Host sender;
    private final Host target;
    private final Long receptionTime;

    @Override
    public String toString() {
        return "IHaveMessage{" +
                "mid=" + mid +
                '}';
    }

    public IHaveMessage(UUID mid, Host sender, Host target) {
        this(mid, sender, target, null);
    }
    
    private IHaveMessage(UUID mid, Host sender, Host target, Long receptionTime) {
        super(MSG_ID);
        this.mid = mid;
        this.sender = sender;
        this.target = target;
        this.receptionTime = receptionTime;
    }

    public UUID getMid() {
        return mid;
    }
    
    public Host getSender() {
        return sender;
    }
    
    public Host getTarget() {
        return target;
    }
    
    public Long getReceptionTime() {
        return receptionTime;
    }

    public static ISerializer<IHaveMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(IHaveMessage iHaveMessage, ByteBuf out) throws IOException {
            out.writeLong(iHaveMessage.mid.getMostSignificantBits());
            out.writeLong(iHaveMessage.mid.getLeastSignificantBits());
            Host.serializer.serialize(iHaveMessage.sender, out);
            Host.serializer.serialize(iHaveMessage.target, out);
        }

        @Override
        public IHaveMessage deserialize(ByteBuf in) throws IOException {
            long firstLong = in.readLong();
            long secondLong = in.readLong();
            UUID mid = new UUID(firstLong, secondLong);
            Host sender = Host.serializer.deserialize(in);
            Host target = Host.serializer.deserialize(in);

            return new IHaveMessage(mid, sender, target, System.currentTimeMillis());
        }
    };
}
