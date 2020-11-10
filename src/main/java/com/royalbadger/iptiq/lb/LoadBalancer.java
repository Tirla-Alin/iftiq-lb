package com.royalbadger.iptiq.lb;

import com.royalbadger.iptiq.lb.balancing.BalancingAlgorithm;
import com.royalbadger.iptiq.lb.balancing.RoundRobinBalancingAlgorithm;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.SECONDS;

public class LoadBalancer {
    private volatile ProviderNode[] availableProviders;
    private volatile ProviderNode[] unavailableProviders;
    private final Object lock;
    private final BalancingAlgorithm balancingAlgorithm;
    private final int maxProviders;
    private final int consecutiveChecksRequired;
    private final int maxNodeLoad;

    private LoadBalancer(BalancingAlgorithm balancingAlgorithm,
                         int maxNodeLoad,
                         int maxProviders,
                         int consecutiveChecksRequired,
                         long healthCheckDelay) {
        this.availableProviders = new ProviderNode[0];
        this.unavailableProviders = new ProviderNode[0];
        this.lock = new Object();
        this.balancingAlgorithm = balancingAlgorithm;
        this.maxProviders = maxProviders;
        this.maxNodeLoad = maxNodeLoad;
        this.consecutiveChecksRequired = consecutiveChecksRequired;
        Executors.newSingleThreadScheduledExecutor().scheduleWithFixedDelay(() -> {
            ProviderNode[] curAvailableProviders;
            ProviderNode[] curUnavailableProviders;
            synchronized (lock) {
                curAvailableProviders = this.availableProviders;
                curUnavailableProviders = this.unavailableProviders;
            }

            for (ProviderNode provider : curAvailableProviders) {
                if (!provider.getProvider().check()) {
                    this.updateAvailability(provider, false);
                    provider.setSuccessChecks(0);
                }
            }

            for (ProviderNode provider : curUnavailableProviders) {
                if (provider.getProvider().check()) {
                    int successChecks = provider.getSuccessChecks();
                    if (successChecks == this.consecutiveChecksRequired - 1) {
                        this.updateAvailability(provider, true);
                    }
                    provider.setSuccessChecks(successChecks + 1);
                } else {
                    provider.setSuccessChecks(0);
                }
            }

        }, 0L, healthCheckDelay, SECONDS);

    }

    public int countAvailableProviders() {
        return availableProviders.length;
    }

    public int countUnavailableProviders() {
        return unavailableProviders.length;
    }

    public int countTotalProviders() {
        synchronized (lock) {
            return availableProviders.length + unavailableProviders.length;
        }
    }

    public String get() {
        ProviderNode[] providers = this.availableProviders;
        if (providers.length == 0) {
            throw new IllegalStateException("No provider available.");
        }

        return providers[balancingAlgorithm.pick(providers.length)].tryGet();

    }

    public void register(Provider provider) {
        synchronized (lock) {
            ProviderNode[] curProviders = availableProviders;
            ProviderNode[] curUnavailableProviders = unavailableProviders;
            int curProvidersLen = curProviders.length;
            int curUnavailableProvidersLen = curUnavailableProviders.length;
            if (curProvidersLen + curUnavailableProvidersLen == maxProviders) {
                throw new IllegalStateException("Maximum capacity reached.");
            }
            ProviderNode[] newProviders = Arrays.copyOf(curProviders, curProvidersLen + 1);
            newProviders[curProvidersLen] = new ProviderNode(provider, maxNodeLoad);

            availableProviders = newProviders;
        }
    }

    public void unregister(Provider provider) {
        synchronized (lock) {
            ProviderNode[] curProviders = availableProviders;
            ProviderNode[] curUnavailableProviders = unavailableProviders;
            int curProvidersLen = curProviders.length;
            int curUnavailableProvidersLen = curProviders.length;
            int index = find(curProviders, provider);
            if (index >= 0) {
                ProviderNode[] newProviders = new ProviderNode[curProvidersLen - 1];
                System.arraycopy(curProviders, 0, newProviders, 0, index);
                System.arraycopy(curProviders, index + 1, newProviders, index, curProvidersLen - index - 1);
                availableProviders = newProviders;
            } else {
                int indexUnavailable = find(curUnavailableProviders, provider);
                if (indexUnavailable >= 0) {
                    ProviderNode[] newUnavailableProviders = new ProviderNode[curUnavailableProvidersLen - 1];
                    System.arraycopy(curUnavailableProviders, 0, newUnavailableProviders, 0, indexUnavailable);
                    System.arraycopy(curUnavailableProviders, indexUnavailable + 1, newUnavailableProviders, indexUnavailable, curUnavailableProvidersLen - indexUnavailable - 1);
                    unavailableProviders = newUnavailableProviders;
                }
            }
        }
    }

    private void updateAvailability(ProviderNode provider, boolean available) {
        synchronized (lock) {
            ProviderNode[] removeFrom = available ? unavailableProviders : availableProviders;
            ProviderNode[] addTo = available ? availableProviders : unavailableProviders;
            int removeFromLen = removeFrom.length;
            int addToLen = addTo.length;
            int index = find(removeFrom, provider.getProvider());
            if (index >= 0) {
                ProviderNode[] clearedList = new ProviderNode[removeFromLen - 1];
                System.arraycopy(removeFrom, 0, clearedList, 0, index);
                System.arraycopy(removeFrom, index + 1, clearedList, index, removeFromLen - index - 1);
                if (available) {
                    unavailableProviders = clearedList;
                } else {
                    availableProviders = clearedList;
                }

                ProviderNode[] updatedList = Arrays.copyOf(addTo, addToLen + 1);
                updatedList[addToLen] = provider;
                if (available) {
                    availableProviders = updatedList;
                } else {
                    unavailableProviders = updatedList;
                }
            }
        }
    }


    private int find(ProviderNode[] curProviders, Provider provider) {
        for (int index = 0; index < curProviders.length; ++index) {
            if (curProviders[index].getProvider().equals(provider)) {
                return index;
            }
        }
        return -1;
    }

    private static class ProviderNode {
        private final Provider provider;
        private volatile int successChecks;
        private final AtomicInteger currentRequests = new AtomicInteger();
        private final int maxLoad;

        public ProviderNode(Provider provider, int maxLoad) {
            this.provider = provider;
            this.successChecks = Integer.MAX_VALUE;
            this.maxLoad = maxLoad;
        }

        public int getSuccessChecks() {
            return successChecks;
        }

        public void setSuccessChecks(int successChecks) {
            this.successChecks = successChecks;
        }

        public Provider getProvider() {
            return provider;
        }

        public String tryGet() {
            try {
                int requests = currentRequests.incrementAndGet();
                if (requests > maxLoad) {
                    throw new IllegalStateException("Too many requests");
                }
                return provider.get();
            } finally {
                currentRequests.decrementAndGet();
            }
        }
    }

    public static class Builder {
        private Supplier<BalancingAlgorithm> balancingAlgorithm = RoundRobinBalancingAlgorithm::new;
        private int maxNodeLoad = 10;
        private int maxProviders = 10;
        private int consecutiveChecksRequired = 2;
        private long healthCheckDelay = 5;

        public Builder withBalancingAlgorithm(Supplier<BalancingAlgorithm> balancingAlgorithm) {
            this.balancingAlgorithm = balancingAlgorithm;
            return this;
        }

        public Builder withMaxNodeLoad(int maxNodeLoad) {
            this.maxNodeLoad = maxNodeLoad;
            return this;
        }

        public Builder withMaxProviders(int maxProviders) {
            this.maxProviders = maxProviders;
            return this;
        }

        public Builder withConsecutiveChecksRequired(int consecutiveChecksRequired) {
            this.consecutiveChecksRequired = consecutiveChecksRequired;
            return this;
        }

        public Builder withHealthCheckDelay(long healthCheckDelay) {
            this.healthCheckDelay = healthCheckDelay;
            return this;
        }

        public LoadBalancer build() {
            return new LoadBalancer(balancingAlgorithm.get(), maxNodeLoad, maxProviders, consecutiveChecksRequired, healthCheckDelay);
        }
    }
}
