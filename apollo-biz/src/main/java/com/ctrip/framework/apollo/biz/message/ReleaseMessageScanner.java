package com.ctrip.framework.apollo.biz.message;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Lists;

/**
 * @author Jason Song(song_s@ctrip.com)
 *  onfig Service有一个线程会每秒扫描一次ReleaseMessage表，看看是否有新的消息记录
 *
 */
public class ReleaseMessageScanner implements InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(ReleaseMessageScanner.class);
  @Autowired
  private BizConfig bizConfig;
  @Autowired
  private ReleaseMessageRepository releaseMessageRepository;
  private int databaseScanInterval;
  private List<ReleaseMessageListener> listeners;
  private ScheduledExecutorService executorService;
  private long maxIdScanned;

  public ReleaseMessageScanner() {
    listeners = Lists.newCopyOnWriteArrayList();
    executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("ReleaseMessageScanner", true));
  }

  @Override
  public void afterPropertiesSet() {
    //默认定时扫描的时间是1s
    databaseScanInterval = bizConfig.releaseMessageScanIntervalInMilli();
    //获得启动时，当前最大的那一条发布id，并记录下来
    maxIdScanned = loadLargestMessageId();
    //创建一个定时任务:在databaseScanInterval时间后启动，每隔databaseScanInterval毫秒执行一次
    // 定时任务做的事情：
    // 1、，
    executorService.scheduleWithFixedDelay(() -> {
      Transaction transaction = Tracer.newTransaction("Apollo.ReleaseMessageScanner", "scanMessage");
      try {
        //定时的扫描发布记录，并通知相应客户端
        scanMessages();
        transaction.setStatus(Transaction.SUCCESS);
      } catch (Throwable ex) {
        transaction.setStatus(ex);
        logger.error("Scan and send message failed", ex);
      } finally {
        transaction.complete();
      }
    }, databaseScanInterval, databaseScanInterval, TimeUnit.MILLISECONDS);

  }

  /**
   * add message listeners for release message
   * @param listener
   * 客户端注册，则添加一个监听器
   */
  public void addMessageListener(ReleaseMessageListener listener) {
    if (!listeners.contains(listener)) {
      listeners.add(listener);
    }
  }

  /**
   * Scan messages, continue scanning until there is no more messages
   *
   * 扫描ReleaseMessaage表，一批一批的扫描，并通知客户端变更
   */
  private void scanMessages() {
    boolean hasMoreMessages = true;
    //每次拉取500条未通知客户端的发布记录，如果没有了，则退出循环
    while (hasMoreMessages && !Thread.currentThread().isInterrupted()) {
      //如果返回false，表示一次性全部拉取出所有的发布记录，没必要继续遍历了，则断开循环
      hasMoreMessages = scanAndSendMessages();
    }
  }

  /**
   * scan messages and send
   *
   * @return whether there are more messages
   *  1、根据当前已扫描的最大的releaseMessage的id，根据id，查询大于当前的最小的前500条的发布记录
   *  2、遍历这些发布消息，并通知客户端
   *
   */
  private boolean scanAndSendMessages() {
    //查询 ReleaseMessage，每次从大于当前缓存的maxIdScanned中取最小的500条。
    List<ReleaseMessage> releaseMessages =
        releaseMessageRepository.findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
    if (CollectionUtils.isEmpty(releaseMessages)) {
      return false;
    }
    //遍历发布消息并通知客户端
    fireMessageScanned(releaseMessages);
    //记录已通知客户端的最大的发布id
    int messageScanned = releaseMessages.size();
    maxIdScanned = releaseMessages.get(messageScanned - 1).getId();
    return messageScanned == 500;
  }

  /**
   * find largest message id as the current start point
   * @return current largest message id
   */
  private long loadLargestMessageId() {
    //根据id倒序获得最大的那条发布配置记录：ReleaseMessage
    ReleaseMessage releaseMessage = releaseMessageRepository.findTopByOrderByIdDesc();
    return releaseMessage == null ? 0 : releaseMessage.getId();
  }

  /**
   * Notify listeners with messages loaded
   * @param messages
   */
  /**
   *
   * @param messages
   *
   * 遍历所有的发布消息和客户端监听器并通知客户端
   */
  private void fireMessageScanned(List<ReleaseMessage> messages) {
    for (ReleaseMessage message : messages) {
      //遍历所有的监听器
      for (ReleaseMessageListener listener : listeners) {
        try {
          //通知客户端
          listener.handleMessage(message, Topics.APOLLO_RELEASE_TOPIC);
        } catch (Throwable ex) {
          Tracer.logError(ex);
          logger.error("Failed to invoke message listener {}", listener.getClass(), ex);
        }
      }
    }
  }
}
