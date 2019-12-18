package com.ctrip.framework.apollo.internals;

import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.enums.ConfigSourceType;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.ConfigFile;
import com.ctrip.framework.apollo.ConfigFileChangeListener;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.enums.PropertyChangeType;
import com.ctrip.framework.apollo.model.ConfigFileChangeEvent;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 * ConfigFile的一个抽象类
 */
public abstract class AbstractConfigFile implements ConfigFile, RepositoryChangeListener {
  private static final Logger logger = LoggerFactory.getLogger(AbstractConfigFile.class);
  /**
   * ExecutorService 对象，用于配置变化时，异步通知 ConfigChangeListener 监听器们
   *
   * 静态属性，所有 Config 共享该线程池。
   */
  private static ExecutorService m_executorService;
  protected final ConfigRepository m_configRepository;
  /**
   * Namespace 的名字
   */
  protected final String m_namespace;
  /***
   * apollo server上配置文件里的配置的本地缓存
   */
  protected final AtomicReference<Properties> m_configProperties;
  /***
   * 用于监听配置文件发生变化的监听器
   */
  private final List<ConfigFileChangeListener> m_listeners = Lists.newCopyOnWriteArrayList();

  private volatile ConfigSourceType m_sourceType = ConfigSourceType.NONE;

  static {
    m_executorService = Executors.newCachedThreadPool(ApolloThreadFactory
        .create("ConfigFile", true));
  }

  /***
   * 新建一个抽象的配置文件对象
   * @param namespace 配置文件对应的namespace
   * @param configRepository 用于长轮询拉取配置的对象
   *
   */
  public AbstractConfigFile(String namespace, ConfigRepository configRepository) {
    m_configRepository = configRepository;
    m_namespace = namespace;
    m_configProperties = new AtomicReference<>();
    initialize();
  }

  /***
   * 1、从远程拉取最新的配置，并更新到本地内存里
   * 2、注册监听器，当配置发生变化会通知过来，调用onRepositoryChange方法，更新配置缓存 `m_configProperties`
   */
  private void initialize() {
    try {
      /***
       * m_configRepository会从远程apollo server拉取 最新的配置，并更新到Config对象里，这里会把它设置到m_configProperties
       */
      m_configProperties.set(m_configRepository.getConfig());
      m_sourceType = m_configRepository.getSourceType();
    } catch (Throwable ex) {
      Tracer.logError(ex);
      logger.warn("Init Apollo Config File failed - namespace: {}, reason: {}.",
          m_namespace, ExceptionUtil.getDetailMessage(ex));
    } finally {
      //register the change listener no matter config repository is working or not
      //so that whenever config repository is recovered, config could get changed
      //把当前config注册到m_configRepository，如果配置发生变化，就会通知当前对象调用onRepositoryChange方法
      m_configRepository.addChangeListener(this);
    }
  }

  /**
   * 获得 m_namespace
   * @return
   */
  @Override
  public String getNamespace() {
    return m_namespace;
  }

  protected abstract void update(Properties newProperties);

  /***
   * 当 ConfigRepository 读取到配置发生变更时，计算配置变更集合，并通知监听器们。
   * @param namespace the namespace of this repository change
   * @param newProperties the properties after change
   */
  @Override
  public synchronized void onRepositoryChange(String namespace, Properties newProperties) {
    // 忽略，若未变更
    if (newProperties.equals(m_configProperties.get())) {
      return;
    }
    // 读取新的 Properties 对象
    Properties newConfigProperties = new Properties();
    newConfigProperties.putAll(newProperties);
    // 获得【旧】值
    String oldValue = getContent();
    // 更新为【新】值
    update(newProperties);
    m_sourceType = m_configRepository.getSourceType();

    String newValue = getContent();
    // 计算变化类型
    PropertyChangeType changeType = PropertyChangeType.MODIFIED;

    if (oldValue == null) {
      changeType = PropertyChangeType.ADDED;
    } else if (newValue == null) {
      changeType = PropertyChangeType.DELETED;
    }
    // 通知监听器们
    this.fireConfigChange(new ConfigFileChangeEvent(m_namespace, oldValue, newValue, changeType));

    Tracer.logEvent("Apollo.Client.ConfigChanges", m_namespace);
  }

  @Override
  public void addChangeListener(ConfigFileChangeListener listener) {
    if (!m_listeners.contains(listener)) {
      m_listeners.add(listener);
    }
  }

  @Override
  public boolean removeChangeListener(ConfigChangeListener listener) {
    return m_listeners.remove(listener);
  }

  @Override
  public ConfigSourceType getSourceType() {
    return m_sourceType;
  }

  /***
   * 触发配置变更监听器们
   * @param changeEvent
   */
  private void fireConfigChange(final ConfigFileChangeEvent changeEvent) {
    // 缓存 ConfigChangeListener 数组
    for (final ConfigFileChangeListener listener : m_listeners) {
      m_executorService.submit(new Runnable() {
        @Override
        public void run() {
          String listenerName = listener.getClass().getName();
          Transaction transaction = Tracer.newTransaction("Apollo.ConfigFileChangeListener", listenerName);
          try {
            // 通知监听器
            listener.onChange(changeEvent);
            transaction.setStatus(Transaction.SUCCESS);
          } catch (Throwable ex) {
            transaction.setStatus(ex);
            Tracer.logError(ex);
            logger.error("Failed to invoke config file change listener {}", listenerName, ex);
          } finally {
            transaction.complete();
          }
        }
      });
    }
  }
}
