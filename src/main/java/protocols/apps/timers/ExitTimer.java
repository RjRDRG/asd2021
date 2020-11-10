package protocols.apps.timers;

import babel.generic.ProtoTimer;

public class ExitTimer extends ProtoTimer {
    public static final short TIMER_ID = 304;

    public ExitTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }
}
