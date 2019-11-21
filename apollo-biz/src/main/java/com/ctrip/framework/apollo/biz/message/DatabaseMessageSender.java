package com.ctrip.framework.apollo.biz.message;

import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.repository.ReleaseMessageRepository;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;
import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Jason Song(song_s@ctrip.com)
 */

/***
 * Admin Service在配置发布后会往ReleaseMessage表插入一条消息记录，消息内容就是配置发布的AppId+Cluster+Namespace
 */
@Component
public class DatabaseMessageSender implements MessageSender {
  private static final Logger logger = LoggerFactory.getLogger(DatabaseMessageSender.class);
  private static final int CLEAN_QUEUE_MAX_SIZE = 100;
  private BlockingQueue<Long> toClean = Queues.newLinkedBlockingQueue(CLEAN_QUEUE_MAX_SIZE);
  private final ExecutorService cleanExecutorService;
  private final AtomicBoolean cleanStopped;

  private final ReleaseMessageRepository releaseMessageRepository;

  public DatabaseMessageSender(final ReleaseMessageRepository releaseMessageRepository) {
    cleanExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create("DatabaseMessageSender", true));
    cleanStopped = new AtomicBoolean(false);
    this.releaseMessageRepository = releaseMessageRepository;
  }

  /***
   * 1、往ReleaseMessage表插入一条消息记录，后续有一个定时任务会扫描该表
   * 2、往队列里添加一条ReleaseMessage的id，用于清除当前appid+clusterid+namespace对应的旧的历史发布记录
   * @param message
   * @param channel
   */
  @Override
  @Transactional
  public void sendMessage(String message, String channel) {
    logger.info("Sending message {} to channel {}", message, channel);
    if (!Objects.equals(channel, Topics.APOLLO_RELEASE_TOPIC)) {
      logger.warn("Channel {} not supported by DatabaseMessageSender!");
      return;
    }

    Tracer.logEvent("Apollo.AdminService.ReleaseMessage", message);
    Transaction transaction = Tracer.newTransaction("Apollo.AdminService", "sendMessage");
    try {
      //往数据库ReleaseMessage表里保存一条记录，后续有一个定时任务会扫描该表
      ReleaseMessage newMessage = releaseMessageRepository.save(new ReleaseMessage(message));
      //向队列里添加一条消息，因为有一个线程会根据这个队列的id，去移除旧的发布记录
      toClean.offer(newMessage.getId());
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      logger.error("Sending message to database failed", ex);
      transaction.setStatus(ex);
      throw ex;
    } finally {
      transaction.complete();
    }
  }

  /***
   * 初始化一个线程，不停的从队列里取消息，如果队列为空，则等待6s
   * 1、轮询的从队列里获得已添加到ReleaseMessage表的记录的id
   * 2、
   */
  @PostConstruct
  private void initialize() {
    cleanExecutorService.submit(() -> {
      while (!cleanStopped.get() && !Thread.currentThread().isInterrupted()) {
        try {
          Long rm = toClean.poll(1, TimeUnit.SECONDS);
          //获得发布的新版本记录
          if (rm != null) {
            cleanMessage(rm);
          } else {// 队列为空，sleep ，避免空跑，占用 CPU
            TimeUnit.SECONDS.sleep(5);
          }
        } catch (Throwable ex) {
          Tracer.logError(ex);
        }
      }
    });
  }

  /****
   * 当有一条关于：appid+clusterId+namespace 发布了新的配置，则会触发定时任务调用该方法：
   *      该方法主要是用于清理同样的：appid+clusterId+namespace 下的过去的发布记录，因为记录当前发布新的了，那么之前旧的就没必要保留了
   * @param id
   */
  private void cleanMessage(Long id) {
    boolean hasMore = true;
    //double check in case the release message is rolled back
    //根据id 从数据库查询发布的记录：ReleaseMessage
    ReleaseMessage releaseMessage = releaseMessageRepository.findById(id).orElse(null);
    if (releaseMessage == null) {
      return;
    }
    //获取到对应的历史的发布记录
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      // 拉取相同消息内容的 100 条的老消息
      // 老消息的定义：比当前消息编号小，即先发送的
      // 按照 id 升序
      List<ReleaseMessage> messages = releaseMessageRepository.findFirst100ByMessageAndIdLessThanOrderByIdAsc(
          releaseMessage.getMessage(), releaseMessage.getId());
      //清理掉当前发布的 appid+clusterId+namespace 对应的过去发布记录，这样只要保留最新的就好了
      releaseMessageRepository.deleteAll(messages);
      // 若拉取不足 100 条，说明无老消息了
      hasMore = messages.size() == 100;

      messages.forEach(toRemove -> Tracer.logEvent(
          String.format("ReleaseMessage.Clean.%s", toRemove.getMessage()), String.valueOf(toRemove.getId())));
    }
  }

  void stopClean() {
    cleanStopped.set(true);
  }
}
