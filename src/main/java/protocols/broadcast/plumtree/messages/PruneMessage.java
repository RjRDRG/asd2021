package protocols.broadcast.plumtree.messages;

import babel.generic.ProtoMessage;
import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;

public class PruneMessage extends ProtoMessage {
    public static final short MSG_ID = 204;

    private final Host sender;

    @Override
    public String toString() {
        return "PruneMessage";
    }

    public PruneMessage(Host sender) {
        super(MSG_ID);
        this.sender = sender;
    }

    public Host getSender() {
        return sender;
    }

    public static ISerializer<PruneMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(PruneMessage pruneMessage, ByteBuf out) throws IOException {
            Host.serializer.serialize(pruneMessage.sender, out);
        }

        @Override
        public PruneMessage deserialize(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);

            return new PruneMessage(sender);
        }
    };
}
