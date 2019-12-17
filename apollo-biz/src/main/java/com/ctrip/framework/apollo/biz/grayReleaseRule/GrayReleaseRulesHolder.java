package com.ctrip.framework.apollo.biz.grayReleaseRule;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import com.ctrip.framework.apollo.biz.config.BizConfig;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.ReleaseMessage;
import com.ctrip.framework.apollo.biz.message.ReleaseMessageListener;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.repository.GrayReleaseRuleRepository;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.GrayReleaseRuleItemDTO;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.ctrip.framework.apollo.tracer.spi.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jason Song(song_s@ctrip.com)
 * GrayReleaseRule 缓存 Holder ，用于提高对 GrayReleaseRule 的读取速度
 */
public class GrayReleaseRulesHolder implements ReleaseMessageListener, InitializingBean {
  private static final Logger logger = LoggerFactory.getLogger(GrayReleaseRulesHolder.class);
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private static final Splitter STRING_SPLITTER =
      Splitter.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR).omitEmptyStrings();

  @Autowired
  private GrayReleaseRuleRepository grayReleaseRuleRepository;
  @Autowired
  private BizConfig bizConfig;
  /***
   * 定时浏览数据库的间隔时间
   */
  private int databaseScanInterval;
  private ScheduledExecutorService executorService;
  //store configAppId+configCluster+configNamespace -> GrayReleaseRuleCache map
  /**
   * GrayReleaseRuleCache 缓存
   *
   * KEY：configAppId+configCluster+configNamespace，KEY 中不包含 BranchName
   * VALUE：GrayReleaseRuleCache 数组
   */
  private Multimap<String, GrayReleaseRuleCache> grayReleaseRuleCache;
  /**
   * GrayReleaseRuleCache 缓存
   *
   * KEY：clientAppId+clientNamespace+ip ，KEY 中不包含 ClusterName
   * VALUE：GrayReleaseRule.id 数组
   */
  private Multimap<String, Long> reversedGrayReleaseRuleCache;
  private AtomicLong loadVersion;

  public GrayReleaseRulesHolder() {
    loadVersion = new AtomicLong();
    grayReleaseRuleCache = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    reversedGrayReleaseRuleCache = Multimaps.synchronizedSetMultimap(HashMultimap.create());
    executorService = Executors.newScheduledThreadPool(1, ApolloThreadFactory
        .create("GrayReleaseRulesHolder", true));
  }

  /***
   * 当实例初始化完后会调用该方法
   * @throws Exception
   * 1、获得定时轮询数据库中的GrayReleaseRule的时间间隔
   * 2、刚启动的时候，第一次拉取 GrayReleaseRuleCache 到缓存
   * 3、定时任务来定时拉取拉取 GrayReleaseRuleCache 到缓存，定时时间 60S
   */
  @Override
  public void afterPropertiesSet() throws Exception {
    populateDataBaseInterval();
    //force sync load for the first time
    periodicScanRules();
    executorService.scheduleWithFixedDelay(this::periodicScanRules,
        getDatabaseScanIntervalSecond(), getDatabaseScanIntervalSecond(), getDatabaseScanTimeUnit()
    );
  }

  @Override
  public void handleMessage(ReleaseMessage message, String channel) {
    logger.info("message received - channel: {}, message: {}", channel, message);
    String releaseMessage = message.getMessage();
    if (!Topics.APOLLO_RELEASE_TOPIC.equals(channel) || Strings.isNullOrEmpty(releaseMessage)) {
      return;
    }
    List<String> keys = STRING_SPLITTER.splitToList(releaseMessage);
    //message should be appId+cluster+namespace
    if (keys.size() != 3) {
      logger.error("message format invalid - {}", releaseMessage);
      return;
    }
    String appId = keys.get(0);
    String cluster = keys.get(1);
    String namespace = keys.get(2);

    List<GrayReleaseRule> rules = grayReleaseRuleRepository
        .findByAppIdAndClusterNameAndNamespaceName(appId, cluster, namespace);

    mergeGrayReleaseRules(rules);
  }

  /***
   * 定时扫描GrayReleaseRule规则
   *
   */
  private void periodicScanRules() {
    Transaction transaction = Tracer.newTransaction("Apollo.GrayReleaseRulesScanner",
        "scanGrayReleaseRules");
    try {
      loadVersion.incrementAndGet();
      // 从数据卷库中，扫描所有 GrayReleaseRules ，并合并到缓存中
      scanGrayReleaseRules();
      transaction.setStatus(Transaction.SUCCESS);
    } catch (Throwable ex) {
      transaction.setStatus(ex);
      logger.error("Scan gray release rule failed", ex);
    } finally {
      transaction.complete();
    }
  }
  /***
   *  查询灰度发布记录对应的发布版本id：对应GrayReleaseRule中的releaseId
   * @param clientAppId 客户端的appId
   * @param clientIp 客户端ip
   * @param configAppId apollo上配置的appId
   * @param configCluster apollo上配置的clusterName
   * @param configNamespaceName apollo上配置的namespace
   * @return
   *
   * 1、根据configAppId+configCluster+configNamespaceName 直接判断本地的灰度发布的缓存，如果有的话，做下一步判断，没有的话，直接返回。
   * 2、判断缓存的灰度发布记录是不是活跃有效状态active？
   *    如果不是的话，则跳过该GrayReleaseRuleCache
   * 3、检查 GrayReleaseRuleCache 针对的客户端规则ip是不是跟当前的客户端ip匹配，匹配的话，就直接返回这个灰度发布规则对应的发布版本ReleaseId.
   */
  public Long findReleaseIdFromGrayReleaseRule(String clientAppId, //客户端的appId
                                               String clientIp, //客户端ip
                                               String configAppId, //apollo上配置的appId
                                               String configCluster, //apollo上配置的clusterName
                                               String configNamespaceName)//apollo上配置的namespace
                                              {
    //获得灰度发布版本的缓存key：configAppId+configCluster+configNamespaceName
    String key = assembleGrayReleaseRuleKey(configAppId, configCluster, configNamespaceName);
    //直接判断本地的灰度发布的缓存，如果有的话，做下一步判断，没有的话，直接返回
    if (!grayReleaseRuleCache.containsKey(key)) {
      return null;
    }
    //create a new list to avoid ConcurrentModificationException
    //如果缓存里存在，则根据key获得灰度的发布记录
    List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
    for (GrayReleaseRuleCache rule : rules) {
      //如果灰度规则状态不是活跃状态，则跳过
      if (rule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {
        continue;
      }
      //如果规则对应的appId和clientIp都匹配，则返回版本id
      if (rule.matches(clientAppId, clientIp)) {
        return rule.getReleaseId();
      }
    }
    return null;
  }

  /**
   * Check whether there are gray release rules for the clientAppId, clientIp, namespace
   * combination. Please note that even there are gray release rules, it doesn't mean it will always
   * load gray releases. Because gray release rules actually apply to one more dimension - cluster.
   */
  public boolean hasGrayReleaseRule(String clientAppId, String clientIp, String namespaceName) {
    return reversedGrayReleaseRuleCache.containsKey(assembleReversedGrayReleaseRuleKey(clientAppId,
        namespaceName, clientIp)) || reversedGrayReleaseRuleCache.containsKey
        (assembleReversedGrayReleaseRuleKey(clientAppId, namespaceName, GrayReleaseRuleItemDTO
            .ALL_IP));
  }

  /***
   * 扫描数据库中的grayReleaseRule
   * 1、每次500条查询grayReleaseRule，直到查询完成
   *
   */
  private void scanGrayReleaseRules() {
    long maxIdScanned = 0;
    boolean hasMore = true;
    // 循环顺序分批加载 GrayReleaseRule ，直到结束或者线程打断
    while (hasMore && !Thread.currentThread().isInterrupted()) {
      // 顺序分批加载 GrayReleaseRule 500 条
      List<GrayReleaseRule> grayReleaseRules = grayReleaseRuleRepository
          .findFirst500ByIdGreaterThanOrderByIdAsc(maxIdScanned);
      if (CollectionUtils.isEmpty(grayReleaseRules)) {
        break;
      }
      // 合并到 GrayReleaseRule 缓存
      mergeGrayReleaseRules(grayReleaseRules);
      // 获得新的 maxIdScanned ，取最后一条记录
      int rulesScanned = grayReleaseRules.size();
      maxIdScanned = grayReleaseRules.get(rulesScanned - 1).getId();
      //batch is 500
      hasMore = rulesScanned == 500;
    }
  }

  /***
   * 根据appid+clusterName+namespaceName进行合并GrayReleaseRuleCache
   * @param grayReleaseRules
   */
  private void mergeGrayReleaseRules(List<GrayReleaseRule> grayReleaseRules) {
    if (CollectionUtils.isEmpty(grayReleaseRules)) {
      return;
    }
    for (GrayReleaseRule grayReleaseRule : grayReleaseRules) {
      /***
       * 如果灰度规则 grayReleaseRule的id是空的，表示还没发布，则忽略
       */
      if (grayReleaseRule.getReleaseId() == null || grayReleaseRule.getReleaseId() == 0) {
        //filter rules with no release id, i.e. never released
        continue;
      }
      //appid+clusterName+namespaceName
      String key = assembleGrayReleaseRuleKey(grayReleaseRule.getAppId(), grayReleaseRule
          .getClusterName(), grayReleaseRule.getNamespaceName());
      //create a new list to avoid ConcurrentModificationException
      List<GrayReleaseRuleCache> rules = Lists.newArrayList(grayReleaseRuleCache.get(key));
      GrayReleaseRuleCache oldRule = null;
      // 获得子 灰度分支Namespace 对应的旧的 GrayReleaseRuleCache 对象
      for (GrayReleaseRuleCache ruleCache : rules) {
        if (ruleCache.getBranchName().equals(grayReleaseRule.getBranchName())) {
          oldRule = ruleCache;
          break;
        }
      }

      //if old rule is null and new rule's branch status is not active, ignore
      //若不存在老的GrayReleaseRuleCache，并且当前的GrayReleaseRule 对应的分支不处于激活( 有效 )状态，则忽略
      if (oldRule == null && grayReleaseRule.getBranchStatus() != NamespaceBranchStatus.ACTIVE) {
        continue;
      }

      //use id comparison to avoid synchronization
      // 若新的 GrayReleaseRule 为新增或更新，进行缓存更新
      if (oldRule == null || grayReleaseRule.getId() > oldRule.getRuleId()) {
        // 添加新的 GrayReleaseRuleCache 到缓存中
        addCache(key, transformRuleToRuleCache(grayReleaseRule));
        if (oldRule != null) {
          removeCache(key, oldRule);
        }
      } else {
        if (oldRule.getBranchStatus() == NamespaceBranchStatus.ACTIVE) {
          //update load version
          oldRule.setLoadVersion(loadVersion.get());
        } else if ((loadVersion.get() - oldRule.getLoadVersion()) > 1) {
          //remove outdated inactive branch rule after 2 update cycles
          removeCache(key, oldRule);
        }
      }
    }
  }

  private void addCache(String key, GrayReleaseRuleCache ruleCache) {
    if (ruleCache.getBranchStatus() == NamespaceBranchStatus.ACTIVE) {
      for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) {
        for (String clientIp : ruleItemDTO.getClientIpList()) {
          reversedGrayReleaseRuleCache.put(assembleReversedGrayReleaseRuleKey(ruleItemDTO
              .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
        }
      }
    }
    grayReleaseRuleCache.put(key, ruleCache);
  }

  private void removeCache(String key, GrayReleaseRuleCache ruleCache) {
    grayReleaseRuleCache.remove(key, ruleCache);
    for (GrayReleaseRuleItemDTO ruleItemDTO : ruleCache.getRuleItems()) {
      for (String clientIp : ruleItemDTO.getClientIpList()) {
        reversedGrayReleaseRuleCache.remove(assembleReversedGrayReleaseRuleKey(ruleItemDTO
            .getClientAppId(), ruleCache.getNamespaceName(), clientIp), ruleCache.getRuleId());
      }
    }
  }

  private GrayReleaseRuleCache transformRuleToRuleCache(GrayReleaseRule grayReleaseRule) {
    Set<GrayReleaseRuleItemDTO> ruleItems;
    try {
      ruleItems = GrayReleaseRuleItemTransformer.batchTransformFromJSON(grayReleaseRule.getRules());
    } catch (Throwable ex) {
      ruleItems = Sets.newHashSet();
      Tracer.logError(ex);
      logger.error("parse rule for gray release rule {} failed", grayReleaseRule.getId(), ex);
    }

    GrayReleaseRuleCache ruleCache = new GrayReleaseRuleCache(grayReleaseRule.getId(),
        grayReleaseRule.getBranchName(), grayReleaseRule.getNamespaceName(), grayReleaseRule
        .getReleaseId(), grayReleaseRule.getBranchStatus(), loadVersion.get(), ruleItems);

    return ruleCache;
  }

  private void populateDataBaseInterval() {
    databaseScanInterval = bizConfig.grayReleaseRuleScanInterval();
  }

  private int getDatabaseScanIntervalSecond() {
    return databaseScanInterval;
  }

  private TimeUnit getDatabaseScanTimeUnit() {
    return TimeUnit.SECONDS;
  }

  /***
   * 获得灰度发布版本的缓存key：configAppId+configCluster+configNamespaceName
   * @param configAppId
   * @param configCluster
   * @param configNamespaceName
   * @return
   */
  private String assembleGrayReleaseRuleKey(String configAppId, String configCluster, String
      configNamespaceName) {
    return STRING_JOINER.join(configAppId, configCluster, configNamespaceName);
  }

  private String assembleReversedGrayReleaseRuleKey(String clientAppId, String
      clientNamespaceName, String clientIp) {
    return STRING_JOINER.join(clientAppId, clientNamespaceName, clientIp);
  }

}
