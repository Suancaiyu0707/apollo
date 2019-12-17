package com.ctrip.framework.apollo.internals;

import java.util.Map;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.build.ApolloInjector;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.spi.ConfigFactory;
import com.ctrip.framework.apollo.spi.ConfigFactoryManager;
import com.google.common.collect.Maps;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 默认配置管理器实现类
 * 当需要获得的 Config 或 ConfigFile 对象不在缓存中时，
 *    通过 ConfigFactoryManager ，获得对应的 ConfigFactory 对象，从而创建 Config 或 ConfigFile 对象。
 */
public class DefaultConfigManager implements ConfigManager {
  private ConfigFactoryManager m_factoryManager;
  /**
   * Config 对象的缓存，key是namespace
   */
  private Map<String, Config> m_configs = Maps.newConcurrentMap();
  /**
   * ConfigFile 对象的缓存，key是namespace
   */
  private Map<String, ConfigFile> m_configFiles = Maps.newConcurrentMap();

  public DefaultConfigManager() {
    m_factoryManager = ApolloInjector.getInstance(ConfigFactoryManager.class);
  }

  /***
   *  根据namespace获得Config
   * @param namespace the namespace
   * @return
   */
  @Override
  public Config getConfig(String namespace) {
    // 获得 Config 对象
    Config config = m_configs.get(namespace);
    // 若不存在，进行创建，保证单例
    if (config == null) {
      synchronized (this) {
        // 获得 Config 对象
        config = m_configs.get(namespace);
        // 若不存在，进行创建
        if (config == null) {
          //根据namespace初始化对应的ConfigFactory
          ConfigFactory factory = m_factoryManager.getFactory(namespace);
          //根据namespace创建对应的 DefaultConfigFactory
          config = factory.create(namespace);
          // 添加到缓存
          m_configs.put(namespace, config);
        }
      }
    }

    return config;
  }

  /**
   *
   * @param namespace the namespace
   * @param configFileFormat the config file format
   *        Properties("properties"), XML("xml"), JSON("json"), YML("yml"), YAML("yaml"), TXT("txt");
   * @return
   */
  @Override
  public ConfigFile getConfigFile(String namespace, ConfigFileFormat configFileFormat) {
    // 拼接 Namespace 名字，就是namepace+后缀名
    String namespaceFileName = String.format("%s.%s", namespace, configFileFormat.getValue());
    //检查缓存里是否已存在对应的ConfigFile，有的话则直接返回
    ConfigFile configFile = m_configFiles.get(namespaceFileName);

    if (configFile == null) {
      synchronized (this) {
        configFile = m_configFiles.get(namespaceFileName);

        if (configFile == null) {
          ConfigFactory factory = m_factoryManager.getFactory(namespaceFileName);

          configFile = factory.createConfigFile(namespaceFileName, configFileFormat);
          m_configFiles.put(namespaceFileName, configFile);
        }
      }
    }

    return configFile;
  }
}
