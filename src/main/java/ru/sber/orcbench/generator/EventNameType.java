package ru.sber.orcbench.generator;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * AUDEI source event families (approx. shares from architecture profile).
 * LAUNCHER ~41%, ESA ~39%, FIND ~10%, LOGON ~10%.
 */
public enum EventNameType {
    LAUNCHER(0.41d),
    ESA(0.39d),
    FIND(0.10d),
    LOGON(0.10d);

    public static final List<String> ALL_VALUES = Collections.unmodifiableList(Arrays.asList(
            LAUNCHER.name(), ESA.name(), FIND.name(), LOGON.name()
    ));

    private final double expectedShare;

    EventNameType(double expectedShare) {
        this.expectedShare = expectedShare;
    }

    public double expectedShare() {
        return expectedShare;
    }

    public static EventNameType fromName(String raw) {
        return EventNameType.valueOf(raw.trim().toUpperCase());
    }
}
