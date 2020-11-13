package protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HyParViewShuffleReply extends HyParViewMessage {

    public static final short MSG_ID = 110;

    private final Set<Host> subset;
    private final Set<Host> ogSubset;

    public HyParViewShuffleReply(int ttl, Host host, Set<Host> subset, Set<Host> ogSubset) {
        super(MSG_ID, ttl, host);
        this.subset = subset;
        this.ogSubset = ogSubset;
    }

    public HyParViewShuffleReply(short msg_id, byte[] content, int ttl, Host host, Set<Host> subset, Set<Host> ogSubset) {
        super(msg_id, content, ttl, host);
        this.subset = subset;
        this.ogSubset = ogSubset;
    }

    public Set<Host> getNodesSubset() {
        return subset;
    }

    public Set<Host> getOGNodesSubset() {
        return ogSubset;
    }

    public static ISerializer<HyParViewShuffleReply> serializer = new ISerializer<HyParViewShuffleReply>() {
        @Override
        public void serialize(HyParViewShuffleReply message, ByteBuf out) throws IOException {
            out.writeInt(message.ttl);
            out.writeShort(message.msg_id);
            Host.serializer.serialize(message.host, out);
            out.writeInt(message.content.length);
            if (message.content.length > 0) {
                out.writeBytes(message.content);
            }
            out.writeInt(message.subset.size());
            for (Host h : message.subset)
                Host.serializer.serialize(h, out);
            out.writeInt(message.ogSubset.size());
            for (Host h: message.ogSubset)
                Host.serializer.serialize(h, out);
        }

        @Override
        public HyParViewShuffleReply deserialize(ByteBuf in) throws IOException {
            int ttl = in.readInt();
            short msg_id = in.readShort();
            Host host = Host.serializer.deserialize(in);
            int size = in.readInt();
            byte[] content = new byte[size];
            if (size > 0)
                in.readBytes(content);

            int subsetSize = in.readInt();
            Set<Host> subset = new HashSet<>(subsetSize, 1);
            for (int i = 0; i < subsetSize; i++)
                subset.add(Host.serializer.deserialize(in));

            int ogSubsetSize = in.readInt();
            Set<Host> ogSubset = new HashSet<>(ogSubsetSize, 1);
            for (int i = 0; i < ogSubsetSize; i++)
                ogSubset.add(Host.serializer.deserialize(in));

            return new HyParViewShuffleReply(msg_id, content, ttl, host, subset, ogSubset);
        }
    };
}
