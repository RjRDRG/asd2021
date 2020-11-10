package protocols.membership.common.notifications;

import java.util.HashSet;
import java.util.Set;

import babel.generic.ProtoNotification;
import network.data.Host;

public class NeighbourUp extends ProtoNotification {

    public static final short NOTIFICATION_ID = 101;

    private final Set<Host> neighbours;

    public NeighbourUp(Host neighbour) {
        super(NOTIFICATION_ID);
        this.neighbours = new HashSet<>();
        neighbours.add(neighbour);
    }
    
    public void addNeighbour(Host neighbour) {
    	this.neighbours.add(neighbour);
    }

    public Set<Host> getNeighbours() {
        return new HashSet<>(this.neighbours);
    }
    
    public int getLength() {
    	return this.neighbours.size();
    }
}
