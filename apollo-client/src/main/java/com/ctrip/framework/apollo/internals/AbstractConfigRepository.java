package com.ctrip.framework.apollo.internals;

import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.util.ExceptionUtil;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 *  一个Namespace 对应一个 ConfigRepository
 */
public abstract class AbstractConfigRepository implements ConfigRepository {
  private static final Logger logger = LoggerFactory.getLogger(AbstractConfigRepository.class);
  //RepositoryChangeListener 数组，用于监听Repository对应的namespace的配置发生变化
  private List<RepositoryChangeListener> m_listeners = Lists.newCopyOnWriteArrayList();

  /***
   * 尝试同步，作为初次的配置缓存初始化。
   * @return 是否同步成功
   */
  protected boolean trySync() {
    try {
      sync();// 同步
      return true;
    } catch (Throwable ex) {
      Tracer.logEvent("ApolloConfigException", ExceptionUtil.getDetailMessage(ex));
      logger
          .warn("Sync config failed, will retry. Repository {}, reason: {}", this.getClass(), ExceptionUtil
              .getDetailMessage(ex));
    }
    // 返回同步失败
    return false;
  }

  /**
   * 同步配置
   */
  protected abstract void sync();

  @Override
  public void addChangeListener(RepositoryChangeListener listener) {
    if (!m_listeners.contains(listener)) {
      m_listeners.add(listener);
    }
  }

  @Override
  public void removeChangeListener(RepositoryChangeListener listener) {
    m_listeners.remove(listener);
  }

  /**
   * namespace的properties发生变化了，触发监听器们
   * @param namespace 目标 namespace
   * @param newProperties 变化后新的properties
   */
  protected void fireRepositoryChange(String namespace, Properties newProperties) {
    for (RepositoryChangeListener listener : m_listeners) {
      try {
        //处理监听的namespace下的properties的变化
        listener.onRepositoryChange(namespace, newProperties);
      } catch (Throwable ex) {
        Tracer.logError(ex);
        logger.error("Failed to invoke repository change listener {}", listener.getClass(), ex);
      }
    }
  }
}
