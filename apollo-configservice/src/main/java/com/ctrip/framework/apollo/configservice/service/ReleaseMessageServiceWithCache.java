package com.ctrip.framework.apollo.configservice.service;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 实现 InitializingBean 和 ReleaseMessageListener 接口，缓存 ReleaseMessage 的 Service 实现类。通过将 ReleaseMessage 缓存在内存中，提高查询性能。
 */
@Service
public class ReleaseMessageServiceWithCache implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageServiceWithCache
      .class);
  private final ReleaseMessageRepository releaseMessageRepository;
  private final BizConfig bizConfig;
  /**
   * 扫描周期
   */
  private int scanInterval;
  /**
   * 扫描周期单位
   */
  private TimeUnit scanIntervalTimeUnit;
  /**
   * 最后扫描到的 ReleaseMessage 的编号
   */
  private volatile long maxIdScanned;
  /**
   * ReleaseMessage 缓存
   *
   * KEY：`ReleaseMessage.message`
   * VALUE：对应的最新的 ReleaseMessage 记录
   */
  private ConcurrentMap<String, ReleaseMessage> releaseMessageCache;
  /**
   * 是否执行扫描任务
   */
  private AtomicBoolean doScan;
  /**
   * ExecutorService 对象
   */
  private ExecutorService executorService;

  public ReleaseMessageServiceWithCache(
      final ReleaseMessageRepository releaseMessageRepository,
      final BizConfig bizConfig) {
    this.releaseMessageRepository = releaseMessageRepository;
    this.bizConfig = bizConfig;
    initialize();
  }

  private void initialize() {
    // 创建缓存对象
    releaseMessageCache = Maps.newConcurrentMap();
    // 设置 doScan 为 true
    doScan = new AtomicBoolean(true);
    // 创建 ScheduledExecutorService 对象，大小为 1 。
    executorService = Executors.newSingleThreadExecutor(ApolloThreadFactory
        .create("ReleaseMessageServiceWithCache", true));
  }

  public ReleaseMessage findLatestReleaseMessageForMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return null;
    }

    long maxReleaseMessageId = 0;
    ReleaseMessage result = null;
    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null && releaseMessage.getId() > maxReleaseMessageId) {
        maxReleaseMessageId = releaseMessage.getId();
        result = releaseMessage;
      }
    }

    return result;
  }

  /**
   * 超着最新的发布版本记录
   * @param messages
   * @return
   */
  public List<ReleaseMessage> findLatestReleaseMessagesGroupByMessages(Set<String> messages) {
    if (CollectionUtils.isEmpty(messages)) {
      return Collections.emptyList();
    }
    List<ReleaseMessage> releaseMessages = Lists.newArrayList();

    for (String message : messages) {
      ReleaseMessage releaseMessage = releaseMessageCache.get(message);
      if (releaseMessage != null) {
        releaseMessages.add(releaseMessage);
      }
    }

    return releaseMessages;
  }

  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
// 关闭增量拉取定时任务的执行
    doScan.set(false);
    logger.info("message received - channel: {}, message: {}", channel, message);

    String content = message.getMessage();
    Tracer.logEvent("Apollo.ReleaseMessageService.UpdateCache", String.valueOf(message.getId()));
    // 仅处理 APOLLO_RELEASE_TOPIC
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(content)) {
      return;
    }
    // 计算 gap
    long gap = message.getId() - maxIdScanned;

    // 若无空缺 gap ，直接合并
    if (gap == 1) {
      mergeReleaseMessage(message);
    } else if (gap > 1) {// 如有空缺 gap ，增量拉取
      loadReleaseMessages(maxIdScanned);
    }
  }

  @Override
  public void afterPropertiesSet() {
    // 从 ServerConfig 中，读取任务的周期配置
    populateDataBaseInterval();
    // 初始拉取 ReleaseMessage 到缓存
    loadReleaseMessages(0);
    // 创建定时任务，增量拉取 ReleaseMessage 到缓存，用以处理初始化期间，产生的 ReleaseMessage 遗漏的问题。
    executorService.submit(() -> {
      while (doScan.get() && !Thread.currentThread().isInterrupted()) {
        Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageServiceWithCache",
            "scanNewReleaseMessages");
        try {
          // 增量拉取 ReleaseMessage 到缓存
          loadReleaseMessages(maxIdScanned);
          transaction.setStatus(Transaction.SUCCESS);
        } catch (Throwable ex) {
          transaction.setStatus(ex);
          logger.error("Scan new release messages failed", ex);
        } finally {
          transaction.complete();
        }
        try {
          scanIntervalTimeUnit.sleep(scanInterval);
        } catch (InterruptedException e) {
          //ignore
        }
      }
    });
  }

  /**
   * 合并到 ReleaseMessage 缓存
   * @param releaseMessage
   */
  private synchronized void mergeReleaseMessage(ReleaseMessage releaseMessage) {
    // 获得对应的 ReleaseMessage 对象
    ReleaseMessage old = releaseMessageCache.get(releaseMessage.getMessage());
    // 若编号更大，进行更新缓存
    if (old == null || releaseMessage.getId() > old.getId()) {
      releaseMessageCache.put(releaseMessage.getMessage(), releaseMessage);
      maxIdScanned = releaseMessage.getId();
    }
  }

  private void loadReleaseMessages(long startId) {
    boolean hasMore = true;
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      //current batch is 500
      // 获得大于 maxIdScanned 的 500 条 ReleaseMessage 记录，按照 id 升序
      List<ReleaseMessage> releaseMessages = releaseMessageRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(startId);
      if (CollectionUtils.isEmpty(releaseMessages)) {
        break;
      }
      // 合并到 ReleaseMessage 缓存
      releaseMessages.forEach(this::mergeReleaseMessage);
      // 获得新的 maxIdScanned ，取最后一条记录
      int scanned = releaseMessages.size();
      startId = releaseMessages.get(scanned - 1).getId();
      // 若拉取不足 500 条，说明无新消息了
      hasMore = scanned == 500;
      logger.info("Loaded {} release messages with startId {}", scanned, startId);
    }
  }

  private void populateDataBaseInterval() {
    scanInterval = bizConfig.releaseMessageCacheScanInterval();
    scanIntervalTimeUnit = bizConfig.releaseMessageCacheScanIntervalTimeUnit();
  }

  //only for test use
  private void reset() {
    executorService.shutdownNow();
    initialize();
    afterPropertiesSet();
  }
}
