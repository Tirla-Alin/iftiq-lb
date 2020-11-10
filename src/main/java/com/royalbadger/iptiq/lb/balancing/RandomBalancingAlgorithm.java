package com.royalbadger.iptiq.lb.balancing;

import java.util.concurrent.ThreadLocalRandom;

public class RandomBalancingAlgorithm implements BalancingAlgorithm {
    @Override
    public int pick(int availableProviders) {
        return ThreadLocalRandom.current().nextInt(0, availableProviders);
    }
}
