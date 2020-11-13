package protocols.broadcast.plumtree.timers;

import babel.generic.ProtoTimer;

public class LazyTimer extends ProtoTimer {

    public static final short TIMER_ID = 165;
    
    public LazyTimer() {
        super(TIMER_ID);
    }
    
    @Override
    public ProtoTimer clone() {
        return this;
    }
}
