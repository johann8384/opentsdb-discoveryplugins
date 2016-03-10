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
import net.opentsdb.utils.Config;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.tools.StartupPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * Created by jcreasy on 3/2/16.
 */
public class ExecutePlugin {
  private static Logger LOG = LoggerFactory.getLogger(ExecutePlugin.class);
  private static final short DEFAULT_FLUSH_INTERVAL = 1000;
  public static void main(String[] args) throws IOException {
    LOG.info("Starting.");

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
    startup = loadStartupPlugin(config);
    LOG.info(startup.version());
    LOG.info("shutting down");
    startup.shutdown();
  }

  private static StartupPlugin loadStartupPlugin(Config config) {
    // load the startup plugin if enabled
    StartupPlugin startup = null;

//    if (config.getBoolean("tsd.startup.enable")) {
      startup = PluginLoader.loadSpecificPlugin(
              config.getString("tsd.startup.plugin"), StartupPlugin.class);
      if (startup == null) {
        startup = loadSpecificPlugin(config.getString("tsd.startup.plugin"), StartupPlugin.class);
        if (startup == null) {
          throw new IllegalArgumentException("Unable to locate startup plugin: " +
                  config.getString("tsd.startup.plugin"));
        }
      }
      try {
        startup.initialize(config);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize startup plugin", e);
      }
      LOG.info("Successfully initialized startup plugin [" +
              startup.getClass().getCanonicalName() + "] version: "
              + startup.version());
//    } else {
//      startup = null;
//    }

    return startup;
  }

  /**
   * @param name
   * @param type
   * @param <T>
   * @return
   */
  private static <T> T loadSpecificPlugin(final String name,
                                          final Class<T> type) {
    LOG.debug("trying to find: " + name);
    if (name.isEmpty()) {
      throw new IllegalArgumentException("Missing plugin name");
    }
    ServiceLoader<T> serviceLoader = ServiceLoader.load(type);
    Iterator<T> it = serviceLoader.iterator();

    if (!it.hasNext()) {
      LOG.warn("Unable to locate any plugins of the type: " + type.getName());
      return null;
    }

    while(it.hasNext()) {
      T plugin = it.next();
      if (plugin.getClass().getName().toString().equals(name) || plugin.getClass().getSuperclass().getName().toString().equals(name)) {
        LOG.debug("matched!");
        return plugin;
      } else {
        LOG.debug(plugin.getClass().getName() + " and " +  plugin.getClass().getSuperclass() + " did not match: " + name);
      }
    }

    LOG.warn("Unable to locate locate plugin: " + name);
    return null;
  }
}