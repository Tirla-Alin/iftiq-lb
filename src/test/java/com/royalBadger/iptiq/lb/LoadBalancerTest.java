package com.royalBadger.iptiq.lb;

import com.royalbadger.iptiq.lb.LoadBalancer;
import com.royalbadger.iptiq.lb.Provider;
import com.royalbadger.iptiq.lb.balancing.RandomBalancingAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.internal.stubbing.answers.AnswersWithDelay;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@RunWith(JUnitPlatform.class)
public class LoadBalancerTest {
    @Test
    void testProviderGeneratesSameIdentifierEveryTime() {
        Provider provider = new Provider();
        assertEquals(provider.get(), provider.get(), "Same provider returned different identifier");
    }

    @Test
    void testProviderGeneratesUniqueIdentifier() {
        assertNotEquals(new Provider().get(), new Provider().get(), "Two different providers returned same identifier");
    }

    @Test
    void testRegisterProvidersSuccess() {
        int maxProviders = 10;
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withMaxProviders(maxProviders)
                .build();
        for (int index = 0; index < maxProviders; index++) {
            loadBalancer.register(new Provider());
        }
        assertEquals(10, loadBalancer.countAvailableProviders());
    }

    @Test
    void testRegisterTooManyProviders() {
        int maxProviders = 10;
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withMaxProviders(maxProviders)
                .build();
        for (int index = 0; index < maxProviders; index++) {
            loadBalancer.register(new Provider());
        }
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> loadBalancer.register(new Provider()));
        assertEquals("Maximum capacity reached.", exception.getMessage());
    }

    @Test
    void testRandomLoadBalancer() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withBalancingAlgorithm(RandomBalancingAlgorithm::new)
                .build();
        Provider provider1 = new Provider();
        Provider provider2 = new Provider();
        loadBalancer.register(provider1);
        loadBalancer.register(provider2);
        Map<String, Integer> distribution = new HashMap<>();
        distribution.put(provider1.get(), 0);
        distribution.put(provider2.get(), 0);

        for (int index = 0; index < 100; index++) {
            distribution.compute(loadBalancer.get(), (k, v) -> v + 1);
        }
        assertNotEquals(0, distribution.get(provider1.get()), "One of the providers was not called once in 100 requests.");
        assertNotEquals(0, distribution.get(provider2.get()), "One of the providers was not called once in 100 requests.");
    }

    @Test
    void testRoundRobinLoadBalancer() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .build();
        List<Provider> providers = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            Provider provider = new Provider();
            providers.add(provider);
            loadBalancer.register(provider);
        }
        for (int i = 0; i < 10; i++) {
            assertEquals(providers.get(i % 4).get(), loadBalancer.get(), "Round robin did not return expected id.");
        }
    }

    @Test
    void testIncludeExcludeProvider() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .build();
        Provider provider = new Provider();
        loadBalancer.register(provider);

        assertEquals(1, loadBalancer.countTotalProviders());

        loadBalancer.unregister(provider);

        assertEquals(0, loadBalancer.countTotalProviders());
    }

    @Test
    void testHeartBeatMethodIsCalled() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withHealthCheckDelay(1)
                .build();
        Provider provider = Mockito.mock(Provider.class);
        doReturn(false).when(provider).check();
        loadBalancer.register(provider);

        verify(provider, timeout(2000)).check();
        reset(provider);
        verify(provider, timeout(2000)).check();
        assertEquals(0, loadBalancer.countAvailableProviders());
        assertEquals(1, loadBalancer.countUnavailableProviders());
    }

    @Test
    void testNodeIsReEnabledAfterSuccessHeartBeat() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withHealthCheckDelay(1)
                .build();
        Provider provider = Mockito.mock(Provider.class);
        doReturn(false).when(provider).check();
        loadBalancer.register(provider);

        verify(provider, timeout(2000)).check();
        reset(provider);
        doReturn(false).when(provider).check();
        verify(provider, timeout(2000)).check();
        reset(provider);
        doReturn(true).when(provider).check();


        assertEquals(0, loadBalancer.countAvailableProviders());
        assertEquals(1, loadBalancer.countUnavailableProviders());

        verify(provider, timeout(2000)).check();
        reset(provider);
        doReturn(true).when(provider).check();
        verify(provider, timeout(2000)).check();
        reset(provider);
        doReturn(true).when(provider).check();
        verify(provider, timeout(2000)).check();

        assertEquals(1, loadBalancer.countAvailableProviders());
        assertEquals(0, loadBalancer.countUnavailableProviders());
    }

    @Test
    public void testClusterCapacity() {
        LoadBalancer loadBalancer = new LoadBalancer.Builder()
                .withMaxNodeLoad(1)
                .build();
        Provider provider = Mockito.mock(Provider.class);
        doAnswer(new AnswersWithDelay(1000L, (ignore) -> "some-id")).when(provider).get();
        loadBalancer.register(provider);

        new Thread(() -> {
            assertEquals("some-id", loadBalancer.get());
        }).start();
        new Thread(() -> {
            IllegalStateException exception = assertThrows(IllegalStateException.class, loadBalancer::get);
            assertEquals("Too many requests", exception.getMessage());
        }).start();

        verify(provider, times(1)).get();
    }
}
