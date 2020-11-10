package protocols.apps.timers;

import babel.generic.ProtoTimer;

public class BroadcastTimer extends ProtoTimer {
    public static final short TIMER_ID = 301;

    public BroadcastTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
