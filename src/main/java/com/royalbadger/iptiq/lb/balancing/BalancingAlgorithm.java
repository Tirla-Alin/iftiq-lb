package com.royalbadger.iptiq.lb.balancing;

public interface BalancingAlgorithm {
    int pick(int availableProviders);
}
