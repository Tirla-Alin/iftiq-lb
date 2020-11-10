package com.royalbadger.iptiq.lb;

import java.util.Objects;
import java.util.UUID;

public class Provider {
    private final String providerId;

    public Provider() {
        this.providerId = UUID.randomUUID().toString();
    }

    public String get() {
        return providerId;
    }

    public boolean check() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Provider)) return false;
        Provider provider = (Provider) o;
        return providerId.equals(provider.providerId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(providerId);
    }
}
