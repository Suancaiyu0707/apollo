package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.NamespaceLock;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.entity.ReleaseHistory;
import com.ctrip.framework.apollo.biz.repository.ReleaseRepository;
import com.ctrip.framework.apollo.biz.utils.ReleaseKeyGenerator;
import com.ctrip.framework.apollo.common.constants.GsonType;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.time.FastDateFormat;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Service
public class ReleaseService {

  private static final FastDateFormat TIMESTAMP_FORMAT = FastDateFormat.getInstance("yyyyMMddHHmmss");
  private static final Gson gson = new Gson();
  private static final Set<Integer> BRANCH_RELEASE_OPERATIONS = Sets
      .newHashSet(ReleaseOperation.GRAY_RELEASE, ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY);
  private static final Pageable FIRST_ITEM = PageRequest.of(0, 1);
  private static final Type OPERATION_CONTEXT_TYPE_REFERENCE = new TypeToken<Map<String, Object>>() { }.getType();

  private final ReleaseRepository releaseRepository;
  private final ItemService itemService;
  private final AuditService auditService;
  private final NamespaceLockService namespaceLockService;
  private final NamespaceService namespaceService;
  private final NamespaceBranchService namespaceBranchService;
  private final ReleaseHistoryService releaseHistoryService;
  private final ItemSetService itemSetService;

  public ReleaseService(
      final ReleaseRepository releaseRepository,
      final ItemService itemService,
      final AuditService auditService,
      final NamespaceLockService namespaceLockService,
      final NamespaceService namespaceService,
      final NamespaceBranchService namespaceBranchService,
      final ReleaseHistoryService releaseHistoryService,
      final ItemSetService itemSetService) {
    this.releaseRepository = releaseRepository;
    this.itemService = itemService;
    this.auditService = auditService;
    this.namespaceLockService = namespaceLockService;
    this.namespaceService = namespaceService;
    this.namespaceBranchService = namespaceBranchService;
    this.releaseHistoryService = releaseHistoryService;
    this.itemSetService = itemSetService;
  }

  public Release findOne(long releaseId) {
    return releaseRepository.findById(releaseId).orElse(null);
  }


  public Release findActiveOne(long releaseId) {
    return releaseRepository.findByIdAndIsAbandonedFalse(releaseId);
  }

  public List<Release> findByReleaseIds(Set<Long> releaseIds) {
    Iterable<Release> releases = releaseRepository.findAllById(releaseIds);
    if (releases == null) {
      return Collections.emptyList();
    }
    return Lists.newArrayList(releases);
  }

  public List<Release> findByReleaseKeys(Set<String> releaseKeys) {
    return releaseRepository.findByReleaseKeyIn(releaseKeys);
  }
  //获得最后有效的 Release 对象
  public Release findLatestActiveRelease(Namespace namespace) {
    return findLatestActiveRelease(namespace.getAppId(),
                                   namespace.getClusterName(), namespace.getNamespaceName());

  }
  //获得最后有效的 Release 对象
  public Release findLatestActiveRelease(String appId, String clusterName, String namespaceName) {
    return releaseRepository.findFirstByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId,
                                                                                                            clusterName,
                                                                                                            namespaceName);
  }

  public List<Release> findAllReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release> releases = releaseRepository.findByAppIdAndClusterNameAndNamespaceNameOrderByIdDesc(appId,
                                                                                                      clusterName,
                                                                                                      namespaceName,
                                                                                                      page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  public List<Release> findActiveReleases(String appId, String clusterName, String namespaceName, Pageable page) {
    List<Release>
        releases =
        releaseRepository.findByAppIdAndClusterNameAndNamespaceNameAndIsAbandonedFalseOrderByIdDesc(appId, clusterName,
                                                                                                    namespaceName,
                                                                                                    page);
    if (releases == null) {
      return Collections.emptyList();
    }
    return releases;
  }

  @Transactional
  public Release mergeBranchChangeSetsAndRelease(Namespace namespace, String branchName, String releaseName,
                                                 String releaseComment, boolean isEmergencyPublish,
                                                 ItemChangeSets changeSets) {

    checkLock(namespace, isEmergencyPublish, changeSets.getDataChangeLastModifiedBy());

    itemSetService.updateSet(namespace, changeSets);

    Release branchRelease = findLatestActiveRelease(namespace.getAppId(), branchName, namespace
        .getNamespaceName());
    long branchReleaseId = branchRelease == null ? 0 : branchRelease.getId();

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Map<String, Object> operationContext = Maps.newHashMap();
    operationContext.put(ReleaseOperationContext.SOURCE_BRANCH, branchName);
    operationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, branchReleaseId);
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);

    return masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                         changeSets.getDataChangeLastModifiedBy(),
                         ReleaseOperation.GRAY_RELEASE_MERGE_TO_MASTER, operationContext);

  }

  /***
   * 发布配置
   * @param namespace 命名空间
   * @param releaseName 发布名称
   * @param releaseComment 发布描述
   * @param operator 操作人
   * @param isEmergencyPublish 是否紧急发布
   * @return
   */
  @Transactional
  public Release publish(Namespace namespace, String releaseName, String releaseComment,
                         String operator, boolean isEmergencyPublish) {
    //检验 namespace 的锁定，如果是紧急发布的话，则不检验。检验发布人和编辑的人是否同一个
    checkLock(namespace, isEmergencyPublish, operator);
    // 获得 Namespace 的普通配置 Map，从这里我们可以发现，都是String,String
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);
    // 获得父 Namespace
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {//若有父 Namespace ，则是子 Namespace ，进行灰度发布
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
                                    releaseName, releaseComment, operator, isEmergencyPublish);
    }
    // 获得子 Namespace 对象
    Namespace childNamespace = namespaceService.findChildNamespace(namespace);
    Release previousRelease = null;
    if (childNamespace != null) {
      // 获得上一次，并且有效的 Release 对象
      previousRelease = findLatestActiveRelease(namespace);
    }

    //master release
    //创建操作 Context
    Map<String, Object> operationContext = Maps.newHashMap();
    operationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);
    // 主干进行发布，创建的 Namespace ，默认就是主干，而灰度发布使用的是分支
    Release release = masterRelease(namespace, releaseName, releaseComment, operateNamespaceItems,
                                    operator, ReleaseOperation.NORMAL_RELEASE, operationContext);
    // 若有子 Namespace 时，自动将主干合并到子 Namespace ，并进行一次子 Namespace 的发布
    //merge to branch and auto release
    if (childNamespace != null) {
      mergeFromMasterAndPublishBranch(namespace, childNamespace, operateNamespaceItems,
                                      releaseName, releaseComment, operator, previousRelease,
                                      release, isEmergencyPublish);
    }

    return release;
  }

  /***
   * 进行灰度(分支)发布
   * @param parentNamespace 父namespace
   * @param childNamespace 灰度的 namespace
   * @param childNamespaceItems 当前灰度的namespace中的配置
   * @param releaseName 发布名称
   * @param releaseComment 发布描述
   * @param operator 操作人
   * @param isEmergencyPublish 是否紧急
   * @param grayDelKeys
   * @return
   *
   */
  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {
    //获得最后有效的 Release 对象
    Release parentLatestRelease = findLatestActiveRelease(parentNamespace);
    //将最后有效的 专程map
    Map<String, String> parentConfigurations = parentLatestRelease != null ?
            gson.fromJson(parentLatestRelease.getConfigurations(),
                    GsonType.CONFIG) : new HashMap<>();

    //表示否是最近有效的  Release 对象
    long baseReleaseId = parentLatestRelease == null ? 0 : parentLatestRelease.getId();

    Map<String, String> configsToPublish = mergeConfiguration(parentConfigurations, childNamespaceItems);

    if(!(grayDelKeys == null || grayDelKeys.size()==0)){
      for (String key : grayDelKeys){
        configsToPublish.remove(key);
      }
    }

    return branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
        configsToPublish, baseReleaseId, operator, ReleaseOperation.GRAY_RELEASE, isEmergencyPublish,
        childNamespaceItems.keySet());

  }

  @Transactional
  public Release grayDeletionPublish(Namespace namespace, String releaseName, String releaseComment,
                                     String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {

    checkLock(namespace, isEmergencyPublish, operator);

    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);

    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
              releaseName, releaseComment, operator, isEmergencyPublish, grayDelKeys);
    }else {
      throw new NotFoundException("Parent namespace not found");
    }
  }

  /***
   *  校验锁定，如果是紧急发布的，不检验。这里检验主要是检验编辑配置的人和点击发布的人是否是同一个人
   * @param namespace
   * @param isEmergencyPublish
   * @param operator
   */
  private void checkLock(Namespace namespace, boolean isEmergencyPublish, String operator) {
    //每个命名空间维护一个lock
    //如果不是紧急发布，根据命名空间找到对应的lock
    if (!isEmergencyPublish) {// 非紧急发布
      // 获得 NamespaceLock 对象锁，通过数据库记录来实现锁(好蛋疼)
      NamespaceLock lock = namespaceLockService.findLock(namespace.getId());
      //如果找不到锁，判断数据变更是否跟操作人是同一个人，如果不是的话，则抛出异常
      if (lock != null && lock.getDataChangeCreatedBy().equals(operator)) {
        throw new BadRequestException("Config can not be published by yourself.");
      }
    }
  }

  private void mergeFromMasterAndPublishBranch(Namespace parentNamespace, Namespace childNamespace,
                                               Map<String, String> parentNamespaceItems,
                                               String releaseName, String releaseComment,
                                               String operator, Release masterPreviousRelease,
                                               Release parentRelease, boolean isEmergencyPublish) {
    //create release for child namespace
    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);

    Map<String, String> childReleaseConfiguration;
    Collection<String> branchReleaseKeys;
    if (childNamespaceLatestActiveRelease != null) {
      childReleaseConfiguration = gson.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    Map<String, String> parentNamespaceOldConfiguration = masterPreviousRelease == null ?
                                                          null : gson.fromJson(masterPreviousRelease.getConfigurations(),
                                                                        GsonType.CONFIG);

    Map<String, String> childNamespaceToPublishConfigs =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceOldConfiguration, parentNamespaceItems,
            childReleaseConfiguration, branchReleaseKeys);

    //compare
    if (!childNamespaceToPublishConfigs.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace, releaseName, releaseComment,
                    childNamespaceToPublishConfigs, parentRelease.getId(), operator,
                    ReleaseOperation.MASTER_NORMAL_RELEASE_MERGE_TO_GRAY, isEmergencyPublish, branchReleaseKeys);
    }

  }

  private Collection<String> getBranchReleaseKeys(long releaseId) {
    Page<ReleaseHistory> releaseHistories = releaseHistoryService
        .findByReleaseIdAndOperationInOrderByIdDesc(releaseId, BRANCH_RELEASE_OPERATIONS, FIRST_ITEM);

    if (!releaseHistories.hasContent()) {
      return null;
    }

    Map<String, Object> operationContext = gson
        .fromJson(releaseHistories.getContent().get(0).getOperationContext(), OPERATION_CONTEXT_TYPE_REFERENCE);

    if (operationContext == null || !operationContext.containsKey(ReleaseOperationContext.BRANCH_RELEASE_KEYS)) {
      return null;
    }

    return (Collection<String>) operationContext.get(ReleaseOperationContext.BRANCH_RELEASE_KEYS);
  }

  /***
   *
   * @param parentNamespace 父namespace
   * @param childNamespace 灰度的 namespace
   * @param childNamespaceItems 当前灰度的namespace中的配置
   * @param releaseName 发布名称
   * @param releaseComment 发布描述
   * @param operator 操作人
   * @param isEmergencyPublish
   * @return
   */
  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish) {
    return publishBranchNamespace(parentNamespace, childNamespace, childNamespaceItems, releaseName, releaseComment,
            operator, isEmergencyPublish, null);

  }

  /***
   * 主干发布
   * @param namespace
   * @param releaseName
   * @param releaseComment
   * @param configurations
   * @param operator
   * @param releaseOperation
   * @param operationContext
   * @return
   */
  private Release masterRelease(Namespace namespace, String releaseName, String releaseComment,
                                Map<String, String> configurations, String operator,
                                int releaseOperation, Map<String, Object> operationContext) {
    // 获得最后有效的 Release 对象
    Release lastActiveRelease = findLatestActiveRelease(namespace);
    long previousReleaseId = lastActiveRelease == null ? 0 : lastActiveRelease.getId();
    // 创建 Release 对象，并保存
    Release release = createRelease(namespace, releaseName, releaseComment,
                                    configurations, operator);
    // 创建 ReleaseHistory 对象，并保存
    releaseHistoryService.createReleaseHistory(namespace.getAppId(), namespace.getClusterName(),
                                               namespace.getNamespaceName(), namespace.getClusterName(),
                                               release.getId(), previousReleaseId, releaseOperation,
                                               operationContext, operator);

    return release;
  }

  private Release branchRelease(Namespace parentNamespace, Namespace childNamespace,
                                String releaseName, String releaseComment,
                                Map<String, String> configurations, long baseReleaseId,
                                String operator, int releaseOperation, boolean isEmergencyPublish, Collection<String> branchReleaseKeys) {
    Release previousRelease = findLatestActiveRelease(childNamespace.getAppId(),
                                                      childNamespace.getClusterName(),
                                                      childNamespace.getNamespaceName());
    long previousReleaseId = previousRelease == null ? 0 : previousRelease.getId();

    Map<String, Object> releaseOperationContext = Maps.newHashMap();
    releaseOperationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, baseReleaseId);
    releaseOperationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);
    releaseOperationContext.put(ReleaseOperationContext.BRANCH_RELEASE_KEYS, branchReleaseKeys);

    Release release =
        createRelease(childNamespace, releaseName, releaseComment, configurations, operator);

    //update gray release rules
    GrayReleaseRule grayReleaseRule = namespaceBranchService.updateRulesReleaseId(childNamespace.getAppId(),
                                                                                  parentNamespace.getClusterName(),
                                                                                  childNamespace.getNamespaceName(),
                                                                                  childNamespace.getClusterName(),
                                                                                  release.getId(), operator);

    if (grayReleaseRule != null) {
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(grayReleaseRule.getRules()));
    }

    releaseHistoryService.createReleaseHistory(parentNamespace.getAppId(), parentNamespace.getClusterName(),
                                               parentNamespace.getNamespaceName(), childNamespace.getClusterName(),
                                               release.getId(),
                                               previousReleaseId, releaseOperation, releaseOperationContext, operator);

    return release;
  }

  private Map<String, String> mergeConfiguration(Map<String, String> baseConfigurations,
                                                 Map<String, String> coverConfigurations) {
    Map<String, String> result = new HashMap<>();
    //copy base configuration
    for (Map.Entry<String, String> entry : baseConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    //update and publish
    for (Map.Entry<String, String> entry : coverConfigurations.entrySet()) {
      result.put(entry.getKey(), entry.getValue());
    }

    return result;
  }

  /***
   * 获得 Namespace 的普通配置 Map
   * @param namespace
   * @return
   */
  private Map<String, String> getNamespaceItems(Namespace namespace) {
    // 读取 Namespace 的 Item 集合
    List<Item> items = itemService.findItemsWithoutOrdered(namespace.getId());
    Map<String, String> configurations = new HashMap<>();
    // 生成普通配置 Map 。过滤掉注释和空行的配置项
    for (Item item : items) {
      if (StringUtils.isEmpty(item.getKey())) {
        continue;
      }
      configurations.put(item.getKey(), item.getValue());
    }

    return configurations;
  }

  private Release createRelease(Namespace namespace, String name, String comment,
                                Map<String, String> configurations, String operator) {
    Release release = new Release();
    release.setReleaseKey(ReleaseKeyGenerator.generateReleaseKey(namespace));
    release.setDataChangeCreatedTime(new Date());
    release.setDataChangeCreatedBy(operator);
    release.setDataChangeLastModifiedBy(operator);
    release.setName(name);
    release.setComment(comment);
    release.setAppId(namespace.getAppId());
    release.setClusterName(namespace.getClusterName());
    release.setNamespaceName(namespace.getNamespaceName());
    release.setConfigurations(gson.toJson(configurations));
    release = releaseRepository.save(release);

    namespaceLockService.unlock(namespace.getId());
    auditService.audit(Release.class.getSimpleName(), release.getId(), Audit.OP.INSERT,
                       release.getDataChangeCreatedBy());

    return release;
  }

  @Transactional
  public Release rollback(long releaseId, String operator) {
    Release release = findOne(releaseId);
    if (release == null) {
      throw new NotFoundException("release not found");
    }
    if (release.isAbandoned()) {
      throw new BadRequestException("release is not active");
    }

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();

    PageRequest page = PageRequest.of(0, 2);
    List<Release> twoLatestActiveReleases = findActiveReleases(appId, clusterName, namespaceName, page);
    if (twoLatestActiveReleases == null || twoLatestActiveReleases.size() < 2) {
      throw new BadRequestException(String.format(
          "Can't rollback namespace(appId=%s, clusterName=%s, namespaceName=%s) because there is only one active release",
          appId,
          clusterName,
          namespaceName));
    }

    release.setAbandoned(true);
    release.setDataChangeLastModifiedBy(operator);

    releaseRepository.save(release);

    releaseHistoryService.createReleaseHistory(appId, clusterName,
                                               namespaceName, clusterName, twoLatestActiveReleases.get(1).getId(),
                                               release.getId(), ReleaseOperation.ROLLBACK, null, operator);

    //publish child namespace if namespace has child
    rollbackChildNamespace(appId, clusterName, namespaceName, twoLatestActiveReleases, operator);

    return release;
  }

  private void rollbackChildNamespace(String appId, String clusterName, String namespaceName,
                                      List<Release> parentNamespaceTwoLatestActiveRelease, String operator) {
    Namespace parentNamespace = namespaceService.findOne(appId, clusterName, namespaceName);
    Namespace childNamespace = namespaceService.findChildNamespace(appId, clusterName, namespaceName);
    if (parentNamespace == null || childNamespace == null) {
      return;
    }

    Release childNamespaceLatestActiveRelease = findLatestActiveRelease(childNamespace);
    Map<String, String> childReleaseConfiguration;
    Collection<String> branchReleaseKeys;
    if (childNamespaceLatestActiveRelease != null) {
      childReleaseConfiguration = gson.fromJson(childNamespaceLatestActiveRelease.getConfigurations(), GsonType.CONFIG);
      branchReleaseKeys = getBranchReleaseKeys(childNamespaceLatestActiveRelease.getId());
    } else {
      childReleaseConfiguration = Collections.emptyMap();
      branchReleaseKeys = null;
    }

    Release abandonedRelease = parentNamespaceTwoLatestActiveRelease.get(0);
    Release parentNamespaceNewLatestRelease = parentNamespaceTwoLatestActiveRelease.get(1);

    Map<String, String> parentNamespaceAbandonedConfiguration = gson.fromJson(abandonedRelease.getConfigurations(),
                                                                              GsonType.CONFIG);

    Map<String, String>
        parentNamespaceNewLatestConfiguration =
        gson.fromJson(parentNamespaceNewLatestRelease.getConfigurations(), GsonType.CONFIG);

    Map<String, String>
        childNamespaceNewConfiguration =
        calculateChildNamespaceToPublishConfiguration(parentNamespaceAbandonedConfiguration,
            parentNamespaceNewLatestConfiguration, childReleaseConfiguration, branchReleaseKeys);

    //compare
    if (!childNamespaceNewConfiguration.equals(childReleaseConfiguration)) {
      branchRelease(parentNamespace, childNamespace,
          TIMESTAMP_FORMAT.format(new Date()) + "-master-rollback-merge-to-gray", "",
          childNamespaceNewConfiguration, parentNamespaceNewLatestRelease.getId(), operator,
          ReleaseOperation.MATER_ROLLBACK_MERGE_TO_GRAY, false, branchReleaseKeys);
    }
  }

  private Map<String, String> calculateChildNamespaceToPublishConfiguration(
      Map<String, String> parentNamespaceOldConfiguration, Map<String, String> parentNamespaceNewConfiguration,
      Map<String, String> childNamespaceLatestActiveConfiguration, Collection<String> branchReleaseKeys) {
    //first. calculate child namespace modified configs

    Map<String, String> childNamespaceModifiedConfiguration = calculateBranchModifiedItemsAccordingToRelease(
        parentNamespaceOldConfiguration, childNamespaceLatestActiveConfiguration, branchReleaseKeys);

    //second. append child namespace modified configs to parent namespace new latest configuration
    return mergeConfiguration(parentNamespaceNewConfiguration, childNamespaceModifiedConfiguration);
  }

  private Map<String, String> calculateBranchModifiedItemsAccordingToRelease(
      Map<String, String> masterReleaseConfigs, Map<String, String> branchReleaseConfigs,
      Collection<String> branchReleaseKeys) {

    Map<String, String> modifiedConfigs = new HashMap<>();

    if (CollectionUtils.isEmpty(branchReleaseConfigs)) {
      return modifiedConfigs;
    }

    // new logic, retrieve modified configurations based on branch release keys
    if (branchReleaseKeys != null) {
      for (String branchReleaseKey : branchReleaseKeys) {
        if (branchReleaseConfigs.containsKey(branchReleaseKey)) {
          modifiedConfigs.put(branchReleaseKey, branchReleaseConfigs.get(branchReleaseKey));
        }
      }

      return modifiedConfigs;
    }

    // old logic, retrieve modified configurations by comparing branchReleaseConfigs with masterReleaseConfigs
    if (CollectionUtils.isEmpty(masterReleaseConfigs)) {
      return branchReleaseConfigs;
    }

    for (Map.Entry<String, String> entry : branchReleaseConfigs.entrySet()) {

      if (!Objects.equals(entry.getValue(), masterReleaseConfigs.get(entry.getKey()))) {
        modifiedConfigs.put(entry.getKey(), entry.getValue());
      }
    }

    return modifiedConfigs;

  }

  @Transactional
  public int batchDelete(String appId, String clusterName, String namespaceName, String operator) {
    return releaseRepository.batchDelete(appId, clusterName, namespaceName, operator);
  }

}
