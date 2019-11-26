package com.ctrip.framework.apollo.configservice.controller;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.utils.EntityManagerUtil;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.configservice.service.ReleaseMessageServiceWithCache;
import com.ctrip.framework.apollo.configservice.util.NamespaceUtil;
import com.ctrip.framework.apollo.configservice.util.WatchKeysUtil;
import com.ctrip.framework.apollo.configservice.wrapper.DeferredResultWrapper;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloConfigNotification;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.async.DeferredResult;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * @author Jason Song(song_s@ctrip.com)
 *客户端会发起一个Http 请求到 Config Service 的 notifications/v2 接口，
 * 该controller就是用来接收处理客户端的通知
 *
 */
@RestController
@RequestMapping("/notifications/v2")
public class NotificationControllerV2 implements ReleaseMessageListener {
  private static final Logger logger = LoggerFactory.getLogger(NotificationControllerV2.class);
    /***
     * key：是被监听的namespace+cluster+namespace
     * value：是一个DeferredResult的包装对象的列表，当监听的key有返回值的时候，就调用这些DeferredResult进行setResult完成回调
     *      这里要注意：Multimap是允许key相同的元素。因为有可能多个客户端在监听同一个key
     *
     */
  private final Multimap<String, DeferredResultWrapper> deferredResults =
      Multimaps.synchronizedSetMultimap(HashMultimap.create());
  private static final Splitter STRING_SPLITTER =
      Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();
  private static final Type notificationsTypeReference =
      new TypeToken<List<ApolloConfigNotification>>() {
      }.getType();

  private final ExecutorService largeNotificationBatchExecutorService;

  private final WatchKeysUtil watchKeysUtil;
  private final ReleaseMessageServiceWithCache releaseMessageService;
  private final EntityManagerUtil entityManagerUtil;
  private final NamespaceUtil namespaceUtil;
  private final Gson gson;
  private final BizConfig bizConfig;

  @Autowired
  public NotificationControllerV2(
      final WatchKeysUtil watchKeysUtil,
      final ReleaseMessageServiceWithCache releaseMessageService,
      final EntityManagerUtil entityManagerUtil,
      final NamespaceUtil namespaceUtil,
      final Gson gson,
      final BizConfig bizConfig) {

    largeNotificationBatchExecutorService = Executors.newSingleThreadExecutor(ApolloThreadFactory.create
        ("NotificationControllerV2", true));
    this.watchKeysUtil = watchKeysUtil;
    this.releaseMessageService = releaseMessageService;
    this.entityManagerUtil = entityManagerUtil;
    this.namespaceUtil = namespaceUtil;
    this.gson = gson;
    this.bizConfig = bizConfig;
  }

  /***
   *
   * @param appId appId
   * @param cluster 集群类型
   * @param notificationsAsString 请求参数，JSON 字符串
   * @param dataCenter 数据中心
   * @param clientIp 客户端ip
   * @return
   * 1、从客户端的请求参数notifications解析出当前监听的版本id,因为客户端可能用到多个namespace,所以这里解析出来可能是一个列表：List<ApolloConfigNotification>
   * 2、为每个客户端拉取请求创建一个DeferredResultWrapper，当客户端没有变更通知时，则利用DeferredResultWrapper，可以挂起客户端请求。
   * 3、过滤下客户端的传过来的namespace的合法性：
   *    namespaceName不能为空
   *    如果同一个请求中的List<ApolloConfigNotification>包含了多个针对同一个Namespace进行监听的ApolloConfigNotification，则只保留ReleaseMessage.id最大的那个即可
   * 4、拼接map，key：监听的namespace，value为watch_key的集合(appId+clusterid+namespace和appId+datacenter+namespace)
   * 5、根据这些watch_key查找它们对应的最新的发布版本releaseMessage表的记录
   *
   *
   * 客户端请求拉取配置变化的通知，如果没有变化的话，则默认挂起60s
   *    返回所配置的，其中发生变化的 Namespace 对应的 ApolloConfigNotification
   *
   * 注意：
   *    客户端请求时，只传递 ApolloConfigNotification 的 namespaceName + notificationId ，不传递 messages 。
   *      notificationId: releaseMessageId，是递增的
   *
   * 客户端会向服务端发起请求，并把客户端当前的配置消息的id（可以理解为版本号）传递并注册到服务端，并同服务端中ReleaseMessage表里对应的Namespace的ReleaseMessage的id进行比较，
   * 如果小于服务端同一个namespace的最大的messaegId，则表示服务端有新的更新，则这个时候，会返回一个ApolloConfigNotification。客户端接收到ApolloConfigNotification后会向
   * 服务端拉取变更配置的请求。
   *
   * 所以当前请求返回一个表示服务端对应的配置是否发生变化的对象。拉取变更是在之后发起的
   */
  @GetMapping
  public DeferredResult<ResponseEntity<List<ApolloConfigNotification>>> pollNotification(
      @RequestParam(value = "appId") String appId,
      @RequestParam(value = "cluster") String cluster,
      @RequestParam(value = "notifications") String notificationsAsString,
      @RequestParam(value = "dataCenter", required = false) String dataCenter,
      @RequestParam(value = "ip", required = false) String clientIp) {
    /**
     * ApolloConfigNotification：
     *    namespace:
     *    notificationId: releaseMessageId，是递增的
     */
    List<ApolloConfigNotification> notifications = null;
    // 解析 notificationsAsString 参数，创建 ApolloConfigNotification 数组。也就说，当有几个 配置发生变化的 Namespace ，返回几个对应的 ApolloConfigNotification 。
    try {
      notifications =
          gson.fromJson(notificationsAsString, notificationsTypeReference);
    } catch (Throwable ex) {
      Tracer.logError(ex);
    }

    if (CollectionUtils.isEmpty(notifications)) {
      throw new BadRequestException("Invalid format of notifications: " + notificationsAsString);
    }
    // 创建 DeferredResultWrapper 对象
    DeferredResultWrapper deferredResultWrapper = new DeferredResultWrapper();
    // 创建 Namespace 的名字的集合。
    Set<String> namespaces = Sets.newHashSet();
    // 创建客户端的通知 Map 。
    //    key 为 Namespace 名，
    //    value 为通知编号 releaseMessage.id。
    Map<String, Long> clientSideNotifications = Maps.newHashMap();
    //过滤下客户端的传过来的namespace的合法性：
    //   namespaceName不能为空
    //    如果同一个请求中的List<ApolloConfigNotification>包含了多个针对同一个Namespace进行监听的ApolloConfigNotification，则只保留ReleaseMessage.id最大的那个即可
    //key：namespcae,  value：ApolloConfigNotification
      Map<String, ApolloConfigNotification> filteredNotifications = filterNotifications(appId, notifications);
    // 循环 ApolloConfigNotification的Map ，初始化上述变量。
    for (Map.Entry<String, ApolloConfigNotification> notificationEntry : filteredNotifications.entrySet()) {
      String normalizedNamespace = notificationEntry.getKey();//namespacename
      ApolloConfigNotification notification = notificationEntry.getValue();
      // 添加到 `namespaces` 中。
      namespaces.add(normalizedNamespace);
      // 添加到 `clientSideNotifications` 中。
      clientSideNotifications.put(normalizedNamespace, notification.getNotificationId());
      // 记录名字被归一化的 Namespace 。因为，最终返回给客户端，使用原始的 Namespace 名字，否则客户端无法识别。
      if (!Objects.equals(notification.getNamespaceName(), normalizedNamespace)) {
        deferredResultWrapper.recordNamespaceNameNormalizedResult(notification.getNamespaceName(), normalizedNamespace);
      }
    }
    //客户端监听的namespaces不能为空
    if (CollectionUtils.isEmpty(namespaces)) {
      throw new BadRequestException("Invalid format of notifications: " + notificationsAsString);
    }
    // 组装 Watch Key Multimap
    //key：namespace
    //value：appId+clusterid+namespace和appId+datacenter+namespace
    Multimap<String, String> watchedKeysMap =
        watchKeysUtil.assembleAllWatchKeys(appId, cluster, namespaces, dataCenter);
    // 生成 Watch Key 集合
    Set<String> watchedKeys = Sets.newHashSet(watchedKeysMap.values());
    // 获得 Watch Key 集合中，每个 Watch Key 对应的最新的 ReleaseMessage 记录。
    List<ReleaseMessage> latestReleaseMessages =
        releaseMessageService.findLatestReleaseMessagesGroupByMessages(watchedKeys);

    /**
     * Manually close the entity manager.
     * Since for async request, Spring won't do so until the request is finished,
     * which is unacceptable since we are doing long polling - means the db connection would be hold
     * for a very long time
     */

    // 手动关闭 EntityManager
    // 因为对于 async 请求，Spring 在请求完成之前不会这样做
    // 这是不可接受的，因为我们正在做长轮询——意味着 db 连接将被保留很长时间。
    // 实际上，下面的过程，我们已经不需要 db 连接，因此进行关闭。
    entityManagerUtil.closeEntityManager();
    // 获得新的 ApolloConfigNotification 通知数组(如果客户端传过来的监听的releaseMessage.id小于对应的namespace当前最新的发布的releaseMessage.id，则添加到这个列表里)
    List<ApolloConfigNotification> newNotifications =
        getApolloConfigNotifications(namespaces, clientSideNotifications, watchedKeysMap,
            latestReleaseMessages);
    if (!CollectionUtils.isEmpty(newNotifications)) {//表示客户端监听的namespace有新发布的版本，则更新到对应的deferredResultWrapper，并返回给客户端
      //若有新的通知，调用 setResult方法，直接设置 DeferredResult 的结果，从而结束长轮询。
      deferredResultWrapper.setResult(newNotifications);
    } else {//若无新的通知
      //注册到 deferredResults 中，等到有配置变更或超时(默认60s)
      deferredResultWrapper
          .onTimeout(() -> logWatchedKeys(watchedKeys, "Apollo.LongPoll.TimeOutKeys"));//注册超时事件
      //注册结束事件。在其内部，
      deferredResultWrapper.onCompletion(() -> {
        //unregister all keys
        for (String key : watchedKeys) {
          //移除注册的 Watch Key DeferredResultWrapper 出 deferredResults 。
          deferredResults.remove(key, deferredResultWrapper);
        }
        logWatchedKeys(watchedKeys, "Apollo.LongPoll.CompletedKeys");
      });
      // 注册 Watch Key + DeferredResultWrapper 到 `deferredResults` 中，等待配置发生变化后通知。详见 `#handleMessage(...)` 方法。
      for (String key : watchedKeys) {
        this.deferredResults.put(key, deferredResultWrapper);
      }

      logWatchedKeys(watchedKeys, "Apollo.LongPoll.RegisteredKeys");
      logger.debug("Listening {} from appId: {}, cluster: {}, namespace: {}, datacenter: {}",
          watchedKeys, appId, cluster, namespaces, dataCenter);
    }

    return deferredResultWrapper.getResult();
  }

  /**
   * 过滤掉不合法的并创建ApolloConfigNotification：
   *      namespace为空
   *
   * @param appId
   * @param notifications
   * @return
   *        key：namespace
   *        value：ApolloConfigNotification
   */
  private Map<String, ApolloConfigNotification> filterNotifications(String appId,
                                                                    List<ApolloConfigNotification> notifications) {
    Map<String, ApolloConfigNotification> filteredNotifications = Maps.newHashMap();
    for (ApolloConfigNotification notification : notifications) {
      if (Strings.isNullOrEmpty(notification.getNamespaceName())) {//namespace为空的过滤掉
        continue;
      }
      //若 Namespace 名以 .properties 结尾，移除该结尾，并设置到 ApolloConfigNotification 中。
      // 例如 application.properties => application 。
      String originalNamespace = namespaceUtil.filterNamespaceName(notification.getNamespaceName());
      notification.setNamespaceName(originalNamespace);
      // 获得归一化的 Namespace 名字。因为，客户端 Namespace 会填写错大小写。
      // 例如，数据库中 Namespace 名为 Fx.Apollo ，而客户端 Namespace 名为 fx.Apollo
      //      通过归一化后，统一为 Fx.Apollo
      String normalizedNamespace = namespaceUtil.normalizeNamespace(appId, originalNamespace);
      // 如果客户端 Namespace 的名字有大小写的问题，并且恰好有不同的通知编号。
      // 例如 Namespace 名字为 FX.apollo 的通知编号是 1 ，但是 fx.apollo 的通知编号为 2 。
      //     我们应该让 FX.apollo 可以更新它的通知编号，
      //     所以，我们使用 FX.apollo 的 ApolloConfigNotification 对象，添加到结果，而忽略 fx.apollo 。

      //filteredNotifications集合里已经有了，但是集合里的通知编号<当前遍历的通知编号，则跳过
      if (filteredNotifications.containsKey(normalizedNamespace) &&
          filteredNotifications.get(normalizedNamespace).getNotificationId() < notification.getNotificationId()) {
        continue;
      }

      filteredNotifications.put(normalizedNamespace, notification);
    }
    return filteredNotifications;
  }

  /**
   *
   * @param namespaces
   * @param clientSideNotifications 客户端传过来的需要监听的releaseMessage.id
   *                                 key：namespace
   *                                value:  nitiificationId(或者说releaseMessage.id)
   * @param watchedKeysMap   监听的namespace和watchKey之间关系
   *                            key：namespace
   *                            value：appId+clusterId+namespace
   * @param latestReleaseMessages watchKey列表对应的最新发布的版本消息
   *
   * @return
   */
  private List<ApolloConfigNotification> getApolloConfigNotifications(Set<String> namespaces,
                                                                      Map<String, Long> clientSideNotifications,
                                                                      Multimap<String, String> watchedKeysMap,
                                                                      List<ReleaseMessage> latestReleaseMessages) {
    // 初始化 ApolloConfigNotification 数组
    List<ApolloConfigNotification> newNotifications = Lists.newArrayList();

    if (!CollectionUtils.isEmpty(latestReleaseMessages)) {//如果这些watchKey最难有发版记录
      //创建最新通知的 Map 。其中 Key 为 Watch Key 。
      Map<String, Long> latestNotifications = Maps.newHashMap();
      for (ReleaseMessage releaseMessage : latestReleaseMessages) {
        latestNotifications.put(releaseMessage.getMessage(), releaseMessage.getId());
      }
      // 循环 Namespace 的名字的集合，判断是否有配置更新，比较客户端的通知编号和服务的最大通知编号，判断对应的namespace是否有变更
      for (String namespace : namespaces) {
        //获得客户端监听的namespace对应的releaseMessage.id
        long clientSideId = clientSideNotifications.get(namespace);
        long latestId = ConfigConsts.NOTIFICATION_ID_PLACEHOLDER;
        // 获得 Namespace 对应的 Watch Key 集合
        Collection<String> namespaceWatchedKeys = watchedKeysMap.get(namespace);
        //// 获得最大的通知编号
        for (String namespaceWatchedKey : namespaceWatchedKeys) {
          long namespaceNotificationId =
              latestNotifications.getOrDefault(namespaceWatchedKey, ConfigConsts.NOTIFICATION_ID_PLACEHOLDER);
          if (namespaceNotificationId > latestId) {
            latestId = namespaceNotificationId;
          }
        }
        // 若服务器的通知编号大于客户端的通知编号，意味着有配置更新
        if (latestId > clientSideId) {
          //创建ApolloConfigNotification
          ApolloConfigNotification notification = new ApolloConfigNotification(namespace, latestId);
          //循环调用，添加通知编号到 ApolloConfigNotification 中。对于关联类型的 Namespace ，details 会是多个。
          namespaceWatchedKeys.stream().filter(latestNotifications::containsKey).forEach(namespaceWatchedKey ->
              notification.addMessage(namespaceWatchedKey, latestNotifications.get(namespaceWatchedKey)));
          //添加 ApolloConfigNotification 对象到结果( newNotifications )。
          newNotifications.add(notification);
        }
      }
    }
    return newNotifications;
  }

  /***
   * 通知监听的客户端开始处理消息。
   * 当有新的 ReleaseMessage 时，通知其对应的 Namespace 的，并且正在等待的请求
   * @param message
   * @param channel
   *
   * 1、从ReleaseMessage发布消息中获得content=appId+clusterName+namespaceName
   * 2、判断是否有客户端监听对应的 content，没有则直接返回
   * 3、如果存在客户端监听对应的content，这里会返回一个DeferredResult列表：因为Multimap是允许key相同的元素，因为有可能多个客户端在监听同一个key。
   * 4、创建 一个用用于通知对应的DeferredResult的 ApolloConfigNotification 对象，并在ApolloConfigNotification存放最新更新的 releaseId和content
   *    (这样客户端收到通知后，就要拿着releaseId和content查找最新的Release发布信息进行更新)
   * 5、循环遍历匹配的DeferredResult，并setResults，这样客户端就会收到响应返回。
   *    若需要通知的客户端过多，使用 ExecutorService 异步通知，避免“惊群效应”
   * 假设一个公共 Namespace 有10W 台机器使用，如果该公共 Namespace 发布时直接下发配置更新消息的话，就会导致这 10W 台机器一下子都来请求配置，
   *  这动静就有点大了，而且对 Config Service 的压力也会比较大。
   */
  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    //获得消息内容，是一个字符串：appId+clusterName+namespaceName,
      // eg:SampleApp+cluster1+application
    String content = message.getMessage();
    Tracer.logEvent("Apollo.LongPoll.Messages", content);
    // 仅处理 APOLLO_RELEASE_TOPIC
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(content)) {
      return;
    }
    //从消息中分解出namespace
    String changedNamespace = retrieveNamespaceFromReleaseMessage.apply(content);
    //namespace不能为空
    if (Strings.isNullOrEmpty(changedNamespace)) {
      logger.error("message format invalid - {}", content);
      return;
    }
    // 判断`deferredResults` 是否存在对应的 Watch Key。也就是如果并未存在客户端监听这个namespace，则返回
    if (!deferredResults.containsKey(content)) {
      return;
    }
    // 如果`deferredResults` 存在对应的 Watch Key，创建 DeferredResultWrapper 数组，避免并发问题。
     // 因为Multimap是允许key相同的元素，因为有可能多个客户端在监听同一个key。
    List<DeferredResultWrapper> results = Lists.newArrayList(deferredResults.get(content));
    // 创建 一个用户通知对应的DeferredResult的 ApolloConfigNotification 对象
    ApolloConfigNotification configNotification = new ApolloConfigNotification(changedNamespace, message.getId());
    configNotification.addMessage(content, message.getId());

    //do async notification if too many clients
    // 若需要通知的客户端过多，使用 ExecutorService 异步通知，避免“惊群效应”
    //数量可通过 ServerConfig "apollo.release-message.notification.batch" 配置，默认 100 。
    if (results.size() > bizConfig.releaseMessageNotificationBatch()) {
      largeNotificationBatchExecutorService.submit(() -> {
        logger.debug("Async notify {} clients for key {} with batch {}", results.size(), content,
            bizConfig.releaseMessageNotificationBatch());
        for (int i = 0; i < results.size(); i++) {
          // 每 N 个客户端，sleep 一段时间。
          if (i > 0 && i % bizConfig.releaseMessageNotificationBatch() == 0) {
            try {
              TimeUnit.MILLISECONDS.sleep(bizConfig.releaseMessageNotificationBatchIntervalInMilli());
            } catch (InterruptedException e) {
              //ignore
            }
          }
          logger.debug("Async notify {}", results.get(i));
          // 设置 DeferredResult 的结果，从而结束长轮询。
          results.get(i).setResult(configNotification);
        }
      });
      return;
    }

    logger.debug("Notify {} clients for key {}", results.size(), content);
    // 设置 DeferredResult 的结果，从而结束长轮询。
    for (DeferredResultWrapper result : results) {
      result.setResult(configNotification);
    }
    logger.debug("Notification completed");
  }

  /**
   * 通过 ReleaseMessage 的消息内容(appid+clusterid+namespace)，获得对应 Namespace 的名字
   */
  private static final Function<String, String> retrieveNamespaceFromReleaseMessage =
      releaseMessage -> {
        if (Strings.isNullOrEmpty(releaseMessage)) {
          return null;
        }
        List<String> keys = STRING_SPLITTER.splitToList(releaseMessage);
        //message should be appId+cluster+namespace
        if (keys.size() != 3) {
          logger.error("message format invalid - {}", releaseMessage);
          return null;
        }
        return keys.get(2);
      };

  private void logWatchedKeys(Set<String> watchedKeys, String eventName) {
    for (String watchedKey : watchedKeys) {
      Tracer.logEvent(eventName, watchedKey);
    }
  }
}

