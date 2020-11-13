package protocols.membership.hyparview.timers;

import babel.generic.ProtoTimer;

public class HyParViewTimer extends ProtoTimer {

    public static final short TIMER_ID = 101;

    public HyParViewTimer() {
        super(TIMER_ID);
    }

    @Override
    public ProtoTimer clone() {
        return this;
    }

}