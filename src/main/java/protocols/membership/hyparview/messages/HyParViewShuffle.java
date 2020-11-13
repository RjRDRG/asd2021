package protocols.membership.hyparview.messages;

import io.netty.buffer.ByteBuf;
import network.ISerializer;
import network.data.Host;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class HyParViewShuffle extends HyParViewMessage {

    public static final short MSG_ID = 109;

    private final Set<Host> subset;

    public HyParViewShuffle(int ttl, Host host, Set<Host> subset) {
        super(MSG_ID, ttl, host);
        this.subset = subset;
    }

    public HyParViewShuffle(short msg_id, byte[] content, int ttl, Host host, Set<Host> subset) {
        super(msg_id, content, ttl, host);
        this.subset = subset;
    }

    public Set<Host> getNodesSubset() {
        return subset;
    }

    public int getNNodes() {
        return subset.size();
    }

    public static ISerializer<HyParViewShuffle> serializer = new ISerializer<HyParViewShuffle>() {
        @Override
        public void serialize(HyParViewShuffle message, ByteBuf out) throws IOException {
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
        }

        @Override
        public HyParViewShuffle deserialize(ByteBuf in) throws IOException {
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

            return new HyParViewShuffle(msg_id, content, ttl, host, subset);
        }
    };
}
