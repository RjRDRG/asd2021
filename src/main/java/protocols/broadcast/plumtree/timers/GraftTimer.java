package protocols.broadcast.plumtree.timers;

import java.util.UUID;

import babel.generic.ProtoTimer;

public class GraftTimer extends ProtoTimer {

    public static final short TIMER_ID = 132;

    private final UUID mid;
    private final long timeout;
    
    public GraftTimer(UUID mid, long timeout) {
        super(TIMER_ID);
        this.mid= mid;
        this.timeout = timeout;
    }

    public UUID getMid() {
    	return mid;
    }

    public long getTimeout() {return timeout;}

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
