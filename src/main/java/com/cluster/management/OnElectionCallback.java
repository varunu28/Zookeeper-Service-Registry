package com.cluster.management;

public interface OnElectionCallback {
    void onElectedToBeLeader();

    void onWorker();
}
