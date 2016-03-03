package io.tsdb.opentsdb.discoveryplugins;
/**
 * Copyright 2015 The DiscoveryPlugins Authors
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import net.opentsdb.tools.ArgP;
import net.opentsdb.tools.BuildData;
import net.opentsdb.tools.StartupPlugin;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by jcreasy on 3/2/16.
 */
public class ExecutePlugin {
  private static Logger LOG = LoggerFactory.getLogger(ExecutePlugin.class);
  private static final short DEFAULT_FLUSH_INTERVAL = 1000;
  public static void main(String[] args) throws IOException {
    LOG.info("Starting.");
    LOG.info(BuildData.revisionString());
    LOG.info(BuildData.buildString());
    final ArgP argp = new ArgP();

    argp.addOption("--config", "PATH",
            "Path to a configuration file"
                    + " (default: Searches for file see docs).");
    args = argp.parse(args);

    // load configuration
    final Config config;
    final String config_file = argp.get("--config", "");
    if (!config_file.isEmpty())
      config = new Config(config_file);
    else
      config = new Config(true);

    StartupPlugin startup = null;
    try {
      startup = loadStartupPlugins(config);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException("Initialization failed", e);
    } catch (Exception e) {
      throw new RuntimeException("Initialization failed", e);
    }
    LOG.info(startup.version());
    LOG.info("shutting down");
    startup.shutdown();
  }
  private static StartupPlugin loadStartupPlugins(Config config) {
    // load the startup plugin if enabled
    StartupPlugin startup = null;

    if (config.getBoolean("tsd.startup.enable")) {
      startup = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.startup.plugin"), StartupPlugin.class);
      if (startup == null) {
        throw new IllegalArgumentException("Unable to locate startup plugin: " +
                config.getString("tsd.startup.plugin"));
      }
      try {
        startup.initialize(config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize startup plugin", e);
      }
      LOG.info("Successfully initialized startup plugin [" +
              startup.getClass().getCanonicalName() + "] version: "
              + startup.version());
    } else {
      startup = null;
    }

    return startup;
  }
}