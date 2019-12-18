package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.enums.ConfigSourceType;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.core.utils.ClassLoaderUtil;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.RateLimiter;


/**
 * @author Jason Song(song_s@ctrip.com)
 * DefaultConfig是AbstractConfig的默认实现，同时也是一个RepositoryChangeListener
 *
 * 为什么 DefaultConfig 实现 RepositoryChangeListener 接口？
 * ConfigRepository 的一个实现类 RemoteConfigRepository ，会从远程 Config Service 加载配置。但是 Config Service 的配置不是一成不变，可以在 Portal 进行修改。
 * 所以 RemoteConfigRepository 会在配置变更时，从 Admin Service 重新加载配置。为了实现 Config 监听配置的变更，所以需要将 DefaultConfig 注册为 ConfigRepository 的监听器。
 * 因此，DefaultConfig 需要实现 RepositoryChangeListener 接口
 */
public class DefaultConfig extends AbstractConfig implements RepositoryChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(DefaultConfig.class);
  /**
   * Namespace 的名字
   */
  private final String m_namespace;
  /**
   * 项目下，Namespace 对应的配置文件的 Properties
   */
  private final Properties m_resourceProperties;
  /**
   * 配置 Properties 的缓存引用
   */
  private final AtomicReference<Properties> m_configProperties;
  /**
   * 配置 Repository
   */
  private final ConfigRepository m_configRepository;
  /**
   * 答应告警限流器。当读取不到属性值，会打印告警日志。通过该限流器，避免打印过多日志。
   */
  private final RateLimiter m_warnLogRateLimiter;

  private volatile ConfigSourceType m_sourceType = ConfigSourceType.NONE;

  /**
   * 创建一个 DefaultConfig对象
   * Constructor.
   *
   * @param namespace        the namespace of this config instance
   * @param configRepository apollo client用于从apollo 服务端读取配置，DefaultConfig 会从 ConfigRepository 中，加载配置 Properties ，并更新到 m_configProperties 中
   *  1、从项目的从META-INF/config/namespace.properties里加载配置
   *  2、初始化从配置中心拉取namespace对应的配置，这里会调用ConfigRepository向apollo server发起http请求。然后更新m_configRepository
   */
  public DefaultConfig(String namespace, ConfigRepository configRepository) {
    m_namespace = namespace;
    m_resourceProperties = loadFromResource(m_namespace);
    m_configRepository = configRepository;
    m_configProperties = new AtomicReference<>();
    m_warnLogRateLimiter = RateLimiter.create(0.017); // 1 warning log output per minute
    initialize();
  }

  /***
   * 1、会调用RemoteConfigRepository从apllo server拉取最新的配置，并更新m_configRepository
   * 2、将自己注册到 ConfigRepository 中，从而实现每次配置发生变更时，更新配置缓存 `m_configProperties` 。
   */
  private void initialize() {
    try {
      updateConfig(
              m_configRepository.getConfig()//会调用RemoteConfigRepository从apllo server拉取最新的配置
              , m_configRepository.getSourceType()//设置 m_configRepository来源
      );
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger.warn("Init Apollo Local Config failed - namespace: {}, reason: {}.",
          m_namespace, ExceptionUtil.getDetailMessage(ex));
    } finally {
      //register the change listener no matter config repository is working or not
      //so that whenever config repository is recovered, config could get changed
      m_configRepository.addChangeListener(this);
    }
  }

  /***
   * 根据key获得对应的值
   * @param key          the property name
   * @param defaultValue the default value when key is not found or any error occurred
   * @return
   * 1、先从启动参数里查找，有的话，直接返回
   * 2、如果启动参数里没有，则从本地内存m_configProperties里查找，有的话，则直接返回。注意这个m_configProperties是在启动后从apollo上拉下来的，后面会定时监听apollo server变化
   * 3、从系统变量里查找，有的话直接返回
   * 4、如果系统变量里没有，则从META-INF/config/namespace.properties拉取
   * 4、如果都没有的话，则返回默认值
   */
  @Override
  public String getProperty(String key, String defaultValue) {
    // step 1: check system properties, i.e. -Dkey=value
    String value = System.getProperty(key);

    // step 2: check local cached properties file
    if (value == null && m_configProperties.get() != null) {
      value = m_configProperties.get().getProperty(key);
    }

    /**
     * step 3: check env variable, i.e. PATH=...
     * normally system environment variables are in UPPERCASE, however there might be exceptions.
     * so the caller should provide the key in the right case
     */
    if (value == null) {
      value = System.getenv(key);
    }

    // step 4: check properties file from classpath
    if (value == null && m_resourceProperties != null) {
      value = (String) m_resourceProperties.get(key);
    }

    if (value == null && m_configProperties.get() == null && m_warnLogRateLimiter.tryAcquire()) {
      logger.warn("Could not load config for namespace {} from Apollo, please check whether the configs are released in Apollo! Return default value now!", m_namespace);
    }

    return value == null ? defaultValue : value;
  }

  @Override
  public Set<String> getPropertyNames() {
    Properties properties = m_configProperties.get();
    if (properties == null) {
      return Collections.emptySet();
    }

    return stringPropertyNames(properties);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }

  private Set<String> stringPropertyNames(Properties properties) {
    //jdk9以下版本Properties#enumerateStringProperties方法存在性能问题，keys() + get(k) 重复迭代, jdk9之后改为entrySet遍历.
    Map<String, String> h = new HashMap<>();
    for (Map.Entry<Object, Object> e : properties.entrySet()) {
      Object k = e.getKey();
      Object v = e.getValue();
      if (k instanceof String && v instanceof String) {
        h.put((String) k, (String) v);
      }
    }
    return h.keySet();
  }

  /***
   * 如果apollo server的配置发生了变更，则会通知
   * @param namespace the namespace of this repository change
   * @param newProperties the properties after change
   * 1、更新本地缓存，并记录变更的配置
   * 2、如果监听的配置发生变化，则会通知 ConfigChangeListener
   */
  @Override
  public synchronized void onRepositoryChange(String namespace, Properties newProperties) {
    if (newProperties.equals(m_configProperties.get())) {
      return;
    }

    ConfigSourceType sourceType = m_configRepository.getSourceType();
    Properties newConfigProperties = new Properties();
    newConfigProperties.putAll(newProperties);
    //更新本地缓存，并记录变更的配置
    Map<String, ConfigChange> actualChanges = updateAndCalcConfigChanges(newConfigProperties, sourceType);

    //check double checked result
    if (actualChanges.isEmpty()) {
      return;
    }

    this.fireConfigChange(new ConfigChangeEvent(m_namespace, actualChanges));

    Tracer.logEvent("Apollo.Client.ConfigChanges", m_namespace);
  }

  private void updateConfig(Properties newConfigProperties, ConfigSourceType sourceType) {
    m_configProperties.set(newConfigProperties);
    m_sourceType = sourceType;
  }

  /***
   * 如果apollo server的配置发生了变化，则会把最新的配置发送偶来
   * @param newConfigProperties
   * @param sourceType
   * @return
   * 1、修改本地内存的m_configProperties里的属性列表
   * 2、清除config里旧的缓存
   * 3、记录变更的配置
   */
  private Map<String, ConfigChange> updateAndCalcConfigChanges(Properties newConfigProperties,
      ConfigSourceType sourceType) {
    //根据最新的配置，计算发生变化的配置列表
    List<ConfigChange> configChanges =
        calcPropertyChanges(m_namespace, m_configProperties.get(), newConfigProperties);

    ImmutableMap.Builder<String, ConfigChange> actualChanges =
        new ImmutableMap.Builder<>();

    /** === Double check since DefaultConfig has multiple config sources ==== **/

    //1. use getProperty to update configChanges's old value
    for (ConfigChange change : configChanges) {
      change.setOldValue(this.getProperty(change.getPropertyName(), change.getOldValue()));
    }

    //2. update m_configProperties
    updateConfig(newConfigProperties, sourceType);
    clearConfigCache();

    //3. use getProperty to update configChange's new value and calc the final changes
    for (ConfigChange change : configChanges) {
      change.setNewValue(this.getProperty(change.getPropertyName(), change.getNewValue()));
      switch (change.getChangeType()) {
        case ADDED:
          if (Objects.equals(change.getOldValue(), change.getNewValue())) {
            break;
          }
          if (change.getOldValue() != null) {
            change.setChangeType(PropertyChangeType.MODIFIED);
          }
          actualChanges.put(change.getPropertyName(), change);
          break;
        case MODIFIED:
          if (!Objects.equals(change.getOldValue(), change.getNewValue())) {
            actualChanges.put(change.getPropertyName(), change);
          }
          break;
        case DELETED:
          if (Objects.equals(change.getOldValue(), change.getNewValue())) {
            break;
          }
          if (change.getNewValue() != null) {
            change.setChangeType(PropertyChangeType.MODIFIED);
          }
          actualChanges.put(change.getPropertyName(), change);
          break;
        default:
          //do nothing
          break;
      }
    }
    return actualChanges.build();
  }

  /***
   *
   * @param namespace
   * @return
   */
  private Properties loadFromResource(String namespace) {
    //从META-INF/config/namespace.properties里加载配置
    String name = String.format("META-INF/config/%s.properties", namespace);
    InputStream in = ClassLoaderUtil.getLoader().getResourceAsStream(name);
    Properties properties = null;

    if (in != null) {
      properties = new Properties();

      try {
        properties.load(in);
      } catch (IOException ex) {
        Tracer.logError(ex);
        logger.error("Load resource config for namespace {} failed", namespace, ex);
      } finally {
        try {
          in.close();
        } catch (IOException ex) {
          // ignore
        }
      }
    }

    return properties;
  }
}
