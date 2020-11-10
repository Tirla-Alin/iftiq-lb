package com.royalbadger.iptiq.lb.balancing;

import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinBalancingAlgorithm implements BalancingAlgorithm {
    private final AtomicInteger position = new AtomicInteger(0);

    @Override
    public int pick(int availableProviders) {
        boolean isDone = false;
        int pickedProvider = -1;
        while (!isDone) {
            int desiredPosition = position.get();
            pickedProvider = desiredPosition >= availableProviders ? 0 : desiredPosition;

            isDone = position.compareAndSet(desiredPosition, pickedProvider + 1);
        }
        return pickedProvider;
    }
}
