package com.cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";

    private final ZooKeeper zooKeeper;
    private String currentZNodeName;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper) throws InterruptedException, KeeperException {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    /**
     * Register the service to the cluster
     *
     * @param metadata metadata of the service to be registered
     * @throws InterruptedException if the current thread is interrupted
     * @throws KeeperException      if the Zookeeper operation fails
     */
    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZNodeName = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    /**
     * Unregister the service from the cluster
     */
    public void unregisterFromCluster() {
        try {
            if (currentZNodeName != null && zooKeeper.exists(currentZNodeName, false) != null) {
                zooKeeper.delete(currentZNodeName, -1);
            }
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Register to get notified for any changes to the cluster
     */
    public void registerForUpdates() {
        try {
            updateAddress();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Retrieve all the service addresses in the cluster
     *
     * @return all the service addresses in the cluster
     * @throws InterruptedException if the current thread is interrupted
     * @throws KeeperException      if the Zookeeper operation fails
     */
    public synchronized List<String> getAllServiceAddresses() throws InterruptedException, KeeperException {
        if (allServiceAddresses == null) {
            updateAddress();
        }
        return allServiceAddresses;
    }

    private void createServiceRegistryZnode() throws InterruptedException, KeeperException {
        if (zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
            try {
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private synchronized void updateAddress() throws InterruptedException, KeeperException {
        List<String> workerNodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerNodes.size());
        for (String workerZnode : workerNodes) {
            String workerZnodeFullPath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodeFullPath, false);
            if (stat == null) {
                continue;
            }
            byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, false, stat);
            String address = new String(addressBytes);
            addresses.add(address);
        }
        this.allServiceAddresses = Collections.unmodifiableList(addresses);
        System.out.println("The cluster addresses are: " + this.allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddress();
        } catch (InterruptedException | KeeperException e) {
            throw new RuntimeException(e);
        }
    }
}
