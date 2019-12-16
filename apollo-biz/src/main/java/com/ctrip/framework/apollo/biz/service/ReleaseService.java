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
   * 1、检验 namespace 的锁定，如果是紧急发布的话，则不检验。检验发布人和编辑的人是否同一个
   * 2、获得当前待发布的namespace下所有的item(包括已发布和未发布的)
   * 3、查找主干的namespace：
   *    如果当前待发布的namespace是一个灰度的分支，则它的parentClusterId就可以查询到父Namespace
   * 4、如果父namespace不为空，也就是当前namespace是灰度的namespace进行发布
   *    a、将主干的已发布的namespace合并到当前灰度分支，维护完整的item.
   *    b、根据完整的item发布一条新的release记录(主干的namespace的发布记录不变)
   *    c、生成一条grayReleaseRule，用于关联限制这条灰度发布记录影响的客户端
   * 5、如果不是一个灰度发布：
   *    a、查找当前主干namespace下的灰度分支(如果有的话，最多也就只会有一条)
   *    b、创建发布版本记录：
   *      发布记录：release和 发布历史记录：releaseHistory
   *    c、如果当前namespace下存在灰度发布分支：
   *       自动将主干合并到子 Namespace ，并进行一次子 Namespace 的发布
   *
   */
  @Transactional
  public Release publish(Namespace namespace, String releaseName, String releaseComment,
                         String operator, boolean isEmergencyPublish) {
    //检验 namespace 的锁定，如果是紧急发布的话，则不检验。检验发布人和编辑的人是否同一个
    checkLock(namespace, isEmergencyPublish, operator);
    // 获得 Namespace 的普通配置 Map，从这里我们可以发现，都是<String,String>
    Map<String, String> operateNamespaceItems = getNamespaceItems(namespace);
    // 获得父 Namespace, 判断是否灰度发布
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);

    //branch release
    if (parentNamespace != null) {//若有父 Namespace ，则是子 Namespace ，进行灰度发布
      //客户端即使在灰度发布的情况下，也是使用 父 Namespace 的 Cluster 名字。也就说，灰度发布，对客户端是透明无感知的
      return publishBranchNamespace(parentNamespace, namespace, operateNamespaceItems,
                                    releaseName, releaseComment, operator, isEmergencyPublish);
    }
    // 获得子 Namespace 。根据当前的namespace查找它的子namespace,也就是它的灰度分支（最新的肯定最多就一个）
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
   *  进行灰度(分支)发布
   *  子 Namespace 会自动继承 父 Namespace 已经发布的配置。若有相同的配置项，使用 子 Namespace 的。配置处理的逻辑上，和关联 Namespace 是一致的
   * @param parentNamespace 父namespace
   * @param childNamespace 灰度的 namespace
   * @param childNamespaceItems 当前灰度的namespace中的配置，不包含主干的item
   * @param releaseName 发布名称
   * @param releaseComment 发布描述
   * @param operator 操作人
   * @param isEmergencyPublish 是否紧急
   * @param grayDelKeys
   * @return
   * 1、获得parent的cluster的最新的发布版本。这个时候configurations只包含已经发布的item，也就是主干的item
   * 2、将主干namespace最近发布的版本Release里的配置key-val和当前灰度的灰度分支上要发布的配置进行合并。
   * 3、将合并后的最新的完整的灰度配置items列表进行发布
   * 4、创建一条灰度发布记录 release
   * 5、同时生成一条灰度发布规则：GrayReleaseRule，用于指定发布的release关联的规则里的客户端列表。
   * 6、整个过程，主干的namespace对应的item并不会改变
   *
   */
  private Release publishBranchNamespace(Namespace parentNamespace, Namespace childNamespace,
                                         Map<String, String> childNamespaceItems,
                                         String releaseName, String releaseComment,
                                         String operator, boolean isEmergencyPublish, Set<String> grayDelKeys) {
    //获得父 namespace 最后有效的 Release 对象
    Release parentLatestRelease = findLatestActiveRelease(parentNamespace);
    //获得父 namespace最后有效的 专程map
    Map<String, String> parentConfigurations = parentLatestRelease != null ?
            gson.fromJson(parentLatestRelease.getConfigurations(),
                    GsonType.CONFIG) : new HashMap<>();

    //表示是有有最近有效的  Release 对象
    //获得parent的cluster的最新的发布版本id
    long baseReleaseId = parentLatestRelease == null ? 0 : parentLatestRelease.getId();
    //将本次要发布的配置和父cluster的配置合并
    Map<String, String> configsToPublish = mergeConfiguration(parentConfigurations, childNamespaceItems);

    if(!(grayDelKeys == null || grayDelKeys.size()==0)){
      for (String key : grayDelKeys){
        configsToPublish.remove(key);
      }
    }
    //灰度发布 新增一条新的GrayReleaseRule，删除一条旧的 GrayReleaseRule
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

  /***
   *
   * @param parentNamespace 主干namespace
   * @param childNamespace 灰度namespace
   * @param parentNamespaceItems 主干的items
   * @param releaseName
   * @param releaseComment
   * @param operator
   * @param masterPreviousRelease
   * @param parentRelease
   * @param isEmergencyPublish
   * 1、查找灰度的最新的发布release
   * 2、将灰度的最新的发布的release里的item解析出来
   * 3、将主干的最新的发布的release和灰度的item里进行合并
   * 4、如果发现合并后的item和原始的灰度里的item不一致，则灰度namespace做一次发布
   */
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
   *  灰度分支配置发布
   * @param parentNamespace 父namespace
   * @param childNamespace 灰度的 namespace
   * @param childNamespaceItems 当前灰度的namespace中的配置，不包含主干的item
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
   * 1、创建一条最新的发布记录：Release
   * 2、创建一条发布历史记录：ReleaseHistory
   *
   */
  private Release masterRelease(Namespace namespace, String releaseName, String releaseComment,
                                Map<String, String> configurations, String operator,
                                int releaseOperation, Map<String, Object> operationContext) {
    // 获得最后有效的 Release 对象（多个灰度版本，只看最后一个）
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

  /***
   * 创建一条Relase发布版本记录和GrayReleaseRule灰度发布记录
   * @param parentNamespace
   * @param childNamespace
   * @param releaseName
   * @param releaseComment
   * @param configurations
   * @param baseReleaseId
   * @param operator
   * @param releaseOperation
   * @param isEmergencyPublish
   * @param branchReleaseKeys
   * @return
   */
  private Release branchRelease(Namespace parentNamespace, Namespace childNamespace,
                                String releaseName, String releaseComment,
                                Map<String, String> configurations, long baseReleaseId,
                                String operator, int releaseOperation, boolean isEmergencyPublish, Collection<String> branchReleaseKeys) {
    // 获得父 Namespace 最后有效的 Release 对象
    Release previousRelease = findLatestActiveRelease(childNamespace.getAppId(),
                                                      childNamespace.getClusterName(),
                                                      childNamespace.getNamespaceName());
    // 获得父 Namespace 最后有效的 Release 对象的编号
    long previousReleaseId = previousRelease == null ? 0 : previousRelease.getId();
    // 创建 Map ，用于 ReleaseHistory 对象的 `operationContext` 属性。
    Map<String, Object> releaseOperationContext = Maps.newHashMap();
    releaseOperationContext.put(ReleaseOperationContext.BASE_RELEASE_ID, baseReleaseId);
    releaseOperationContext.put(ReleaseOperationContext.IS_EMERGENCY_PUBLISH, isEmergencyPublish);
    releaseOperationContext.put(ReleaseOperationContext.BRANCH_RELEASE_KEYS, branchReleaseKeys);
    // 创建子 Namespace 的 Release 对象，并保存
    Release release =
        createRelease(childNamespace, releaseName, releaseComment, configurations, operator);

    // 更新 GrayReleaseRule 的 releaseId 属性（删除旧的，新增新的，并返回新的 GrayReleaseRule）
    // update gray release rules
    GrayReleaseRule grayReleaseRule = namespaceBranchService.updateRulesReleaseId(childNamespace.getAppId(),
                                                                                  parentNamespace.getClusterName(),
                                                                                  childNamespace.getNamespaceName(),
                                                                                  childNamespace.getClusterName(),
                                                                                  release.getId(), operator);
    // 创建 ReleaseHistory 对象，并保存
    if (grayReleaseRule != null) {
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(grayReleaseRule.getRules()));
    }
    //创建一条历史记录 ReleaseHistory
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

  /***
   * 创建发布记录：Relase
   * 创建一条审计日志：audit
   * 释放锁：删除数据库中的记录
   * @param namespace
   * @param name
   * @param comment
   * @param configurations
   * @param operator
   * @return
   */
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
    //添加一条发布记录release
    release = releaseRepository.save(release);
    //释放锁，从刚才数据库里的删掉
    namespaceLockService.unlock(namespace.getId());
    //添加一条审计日志
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
