package com.quarformer.config;

import io.quarkus.runtime.annotations.ConfigPhase;
import io.quarkus.runtime.annotations.ConfigRoot;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

@ConfigMapping(prefix = "quarformer")
@ConfigRoot(phase = ConfigPhase.BUILD_AND_RUN_TIME_FIXED)
public interface QuarformerConfig {

  qluster qluster();
  /**
   * Use the Cluster injected via Quarkus' CDI for defaultConnection.
   */
  interface qluster {
    @WithDefault("false")
    boolean enable();
  }
}
