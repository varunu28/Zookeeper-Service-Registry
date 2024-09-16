package com.cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Objects;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";

    private final ZooKeeper zooKeeper;
    private String currentZNodeName;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    @Override
    public void process(WatchedEvent event) {
        if (Objects.requireNonNull(event.getType()) == Event.EventType.NodeDeleted) {
            try {
                reelectLeader();
            } catch (InterruptedException | KeeperException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Volunteer for leadership by creating an ephemeral sequential zNode
     *
     * @throws InterruptedException if the current thread is interrupted
     * @throws KeeperException      if the Zookeeper operation fails
     */
    public void volunteerForLeadership() throws InterruptedException, KeeperException {
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        String zNodeFullPath = zooKeeper.create(zNodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Created zNode: " + zNodeFullPath);
        this.currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    /**
     * Re-elect the leader by watching the predecessor zNode
     *
     * @throws InterruptedException if the current thread is interrupted
     * @throws KeeperException      if the Zookeeper operation fails
     */
    public void reelectLeader() throws InterruptedException, KeeperException {
        Stat predecessorStat = null;
        String predecessorZNode = "";
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            children.sort(String::compareTo);
            String smallestChild = children.getFirst();
            if (smallestChild.equals(currentZNodeName)) {
                System.out.println("I am the leader");
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("I am not the leader");
                int predecessorIndex = children.indexOf(currentZNodeName) - 1;
                predecessorZNode = children.get(predecessorIndex);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNode, this);
            }
            onElectionCallback.onWorker();
        }
        System.out.println("Watching zNode: " + predecessorZNode);
    }
}
