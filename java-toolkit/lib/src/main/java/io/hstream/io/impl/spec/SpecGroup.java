package io.hstream.io.impl.spec;

import java.util.List;

public interface SpecGroup {
    String name();
    List<SpecProperty> properties();
}
