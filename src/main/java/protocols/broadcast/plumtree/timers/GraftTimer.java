package protocols.broadcast.plumtree.timers;

import java.util.UUID;

import babel.generic.ProtoTimer;

public class GraftTimer extends ProtoTimer {

    public static final short TIMER_ID = 132;

    private final UUID mid;
    
    public GraftTimer(UUID mid) {
        super(TIMER_ID);
        this.mid= mid;
    }

    public UUID getMid() {
    	return mid;
    }
    
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
