package com.ctrip.framework.apollo.configservice.service.config;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;

import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.ReleaseMessageService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

/**
 * config service with guava cache
 *
 * @author Jason Song(song_s@ctrip.com)
 */
public class ConfigServiceWithCache extends AbstractConfigService {
  private static final Logger logger = LoggerFactory.getLogger(ConfigServiceWithCache.class);
  //默认缓存时间，60分钟
  private static final long DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES = 60;//1 hour
  private static final String TRACER_EVENT_CACHE_INVALIDATE = "ConfigCache.Invalidate";
  private static final String TRACER_EVENT_CACHE_LOAD = "ConfigCache.LoadFromDB";
  private static final String TRACER_EVENT_CACHE_LOAD_ID = "ConfigCache.LoadFromDBById";
  private static final String TRACER_EVENT_CACHE_GET = "ConfigCache.Get";
  private static final String TRACER_EVENT_CACHE_GET_ID = "ConfigCache.GetById";
  private static final Splitter STRING_SPLITTER =
      Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();

  @Autowired
  private ReleaseService releaseService;

  @Autowired
  private ReleaseMessageService releaseMessageService;

  private LoadingCache<String, ConfigCacheEntry> configCache;

  private LoadingCache<Long, Optional<Release>> configIdCache;

  private ConfigCacheEntry nullConfigCacheEntry;

  public ConfigServiceWithCache() {
    nullConfigCacheEntry = new ConfigCacheEntry(ConfigConsts.NOTIFICATION_ID_PLACEHOLDER, null);
  }

  /***
   * 通过spring调用，对象创建完后调用initialize方法
   */
  @PostConstruct
  void initialize() {
    //构建一个guava缓存的CacheBuilder
    configCache = CacheBuilder.newBuilder()
        .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES)//设置缓存失效时间60分钟
        .build(new CacheLoader<String, ConfigCacheEntry>() {
          @Override
          public ConfigCacheEntry load(String key) throws Exception {
            List<String> namespaceInfo = STRING_SPLITTER.splitToList(key);
            if (namespaceInfo.size() != 3) {
              Tracer.logError(
                  new IllegalArgumentException(String.format("Invalid cache load key %s", key)));
              return nullConfigCacheEntry;
            }

            Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD, key);
            try {
              //根据key查询最新的ReleaseMessage
              ReleaseMessage latestReleaseMessage = releaseMessageService.findLatestReleaseMessageForMessages(Lists
                  .newArrayList(key));
              //获得最新的，并且有效的 Release 对象
              Release latestRelease = releaseService.findLatestActiveRelease(namespaceInfo.get(0), namespaceInfo.get(1),
                  namespaceInfo.get(2));

              transaction.setStatus(Transaction.SUCCESS);
              //获得通知编号
              long notificationId = latestReleaseMessage == null ? ConfigConsts.NOTIFICATION_ID_PLACEHOLDER : latestReleaseMessage
                  .getId();
              // 若 latestReleaseMessage 和 latestRelease 都为空，返回 nullConfigCacheEntry
              if (notificationId == ConfigConsts.NOTIFICATION_ID_PLACEHOLDER && latestRelease == null) {
                return nullConfigCacheEntry;
              }
              // 创建 ConfigCacheEntry 对象
              return new ConfigCacheEntry(notificationId, latestRelease);
            } catch (Throwable ex) {
              transaction.setStatus(ex);
              throw ex;
            } finally {
              transaction.complete();
            }
          }
        });
    configIdCache = CacheBuilder.newBuilder()
        .expireAfterAccess(DEFAULT_EXPIRED_AFTER_ACCESS_IN_MINUTES, TimeUnit.MINUTES)
        .build(new CacheLoader<Long, Optional<Release>>() {
          @Override
          public Optional<Release> load(Long key) throws Exception {
            Transaction transaction = Tracer.newTransaction(TRACER_EVENT_CACHE_LOAD_ID, String.valueOf(key));
            try {
              Release release = releaseService.findActiveOne(key);

              transaction.setStatus(Transaction.SUCCESS);

              return Optional.ofNullable(release);
            } catch (Throwable ex) {
              transaction.setStatus(ex);
              throw ex;
            } finally {
              transaction.complete();
            }
          }
        });
  }

  @Override
  protected Release findActiveOne(long id, ApolloNotificationMessages clientMessages) {
    Tracer.logEvent(TRACER_EVENT_CACHE_GET_ID, String.valueOf(id));
    return configIdCache.getUnchecked(id).orElse(null);
  }

  @Override
  protected Release findLatestActiveRelease(String appId, String clusterName, String namespaceName,
                                            ApolloNotificationMessages clientMessages) {
    //根据appId+clusterName+namespaceName 生成一个唯一key
    String key = ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName);

    Tracer.logEvent(TRACER_EVENT_CACHE_GET, key);
    //从缓存里获取配置记录ConfigCacheEntry
    ConfigCacheEntry cacheEntry = configCache.getUnchecked(key);

    if (clientMessages != null //clientMessages不为空
            && clientMessages.has(key)
            &&clientMessages.get(key) > cacheEntry.getNotificationId()//如果收到变更通知的NotificationId比缓存里的还大
            ) {
      //缓存里已经失效，所以需要去数据库里获取，并更新缓存
      invalidate(key);
      cacheEntry = configCache.getUnchecked(key);
    }

    return cacheEntry.getRelease();
  }

  private void invalidate(String key) {
    // 清空对应的缓存
    configCache.invalidate(key);
    Tracer.logEvent(TRACER_EVENT_CACHE_INVALIDATE, key);
  }

  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    //仅处理APOLLO_RELEASE_TOPIC
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(message.getMessage())) {
      return;
    }

    try {
      // 清空对应的缓存
      invalidate(message.getMessage());
      // 预热缓存，读取 ConfigCacheEntry 对象，重新从 DB 中加载。
      //warm up the cache
      configCache.getUnchecked(message.getMessage());
    } catch (Throwable ex) {
      //ignore
    }
  }

  /**
   * 配置缓存 Entry
   */
  private static class ConfigCacheEntry {
    /**
     * 通知编号
     */
    private final long notificationId;
    /**
     * Release 对象
     */
    private final Release release;

    public ConfigCacheEntry(long notificationId, Release release) {
      this.notificationId = notificationId;
      this.release = release;
    }

    public long getNotificationId() {
      return notificationId;
    }

    public Release getRelease() {
      return release;
    }
  }
}
