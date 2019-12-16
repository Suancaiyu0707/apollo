package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.Cluster;
import com.ctrip.framework.apollo.biz.entity.GrayReleaseRule;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.repository.GrayReleaseRuleRepository;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.constants.ReleaseOperation;
import com.ctrip.framework.apollo.common.constants.ReleaseOperationContext;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.GrayReleaseRuleItemTransformer;
import com.ctrip.framework.apollo.common.utils.UniqueKeyGenerator;
import com.google.common.collect.Maps;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;

@Service
public class NamespaceBranchService {

  private final AuditService auditService;
  private final GrayReleaseRuleRepository grayReleaseRuleRepository;
  private final ClusterService clusterService;
  private final ReleaseService releaseService;
  private final NamespaceService namespaceService;
  private final ReleaseHistoryService releaseHistoryService;

  public NamespaceBranchService(
      final AuditService auditService,
      final GrayReleaseRuleRepository grayReleaseRuleRepository,
      final ClusterService clusterService,
      final @Lazy ReleaseService releaseService,
      final NamespaceService namespaceService,
      final ReleaseHistoryService releaseHistoryService) {
    this.auditService = auditService;
    this.grayReleaseRuleRepository = grayReleaseRuleRepository;
    this.clusterService = clusterService;
    this.releaseService = releaseService;
    this.namespaceService = namespaceService;
    this.releaseHistoryService = releaseHistoryService;
  }

  /***
   *
   * @param appId
   * @param parentClusterName 用于创建灰度分支的cluster
   * @param namespaceName 用于创建灰度分支的namespaceName
   * @param operator
   * @return
   * 1、查找这个parentClusterName下是否已存在灰度分支(一个namespace下只能有一个灰度分支)
   * 2、检查parentClusterName的有效性，判断是否存在
   * 3、根据parentCluster创建灰度分支对应的子Cluster，并把子Cluster关联到父Cluster上
   * 4、根据子cluster创建一个子namespace
   */
  @Transactional
  public Namespace createBranch(String appId, String parentClusterName, String namespaceName, String operator){
    // 获得子 Namespace 对象
    Namespace childNamespace = findBranch(appId, parentClusterName, namespaceName);
    // 若存在子 Namespace 对象，则抛出 BadRequestException 异常。一个 Namespace 有且仅允许有一个子 Namespace 。
    if (childNamespace != null){
      throw new BadRequestException("namespace already has branch");
    }
    // 获得父 Cluster 对象
    Cluster parentCluster = clusterService.findOne(appId, parentClusterName);
    // 若父 Cluster 对象不存在，抛出 BadRequestException 异常
    if (parentCluster == null || parentCluster.getParentClusterId() != 0) {
      throw new BadRequestException("cluster not exist or illegal cluster");
    }

    // 创建子 Cluster 对象，并根据parentClusterId进行关联
    Cluster childCluster = createChildCluster(appId, parentCluster, namespaceName, operator);
    // 保存子 Cluster 对象
    Cluster createdChildCluster = clusterService.saveWithoutInstanceOfAppNamespaces(childCluster);
    // 创建子 Namespace 对象
    //create child namespace
    childNamespace = createNamespaceBranch(appId, createdChildCluster.getName(),
                                                        namespaceName, operator);
    //创建子namespace
    return namespaceService.save(childNamespace);
  }

  public Namespace findBranch(String appId, String parentClusterName, String namespaceName) {
    return namespaceService.findChildNamespace(appId, parentClusterName, namespaceName);
  }

  public GrayReleaseRule findBranchGrayRules(String appId, String clusterName, String namespaceName,
                                             String branchName) {
    return grayReleaseRuleRepository
        .findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);
  }

  /***
   *
   * @param appId 应用id 10001
   * @param clusterName 集群名称 default
   * @param namespaceName namespace名称 seata-properties
   * @param branchName 分支名称 20191216205727-de14b4e2b10903b9
   * @param newRules GrayReleaseRuleItemDTO{clientAppId=10001, clientIpList=[127.0.0.1, 127.0.0.11]}
   */
  @Transactional
  public void updateBranchGrayRules(String appId, String clusterName, String namespaceName,
                                    String branchName, GrayReleaseRule newRules) {
    doUpdateBranchGrayRules(appId, clusterName, namespaceName, branchName, newRules, true, ReleaseOperation.APPLY_GRAY_RULES);
  }

  /**
   *
   * @param appId 应用id 10001
   * @param clusterName 集群名称 default
   * @param namespaceName namespace名称 seata-properties
   * @param branchName 分支名称 20191216205727-de14b4e2b10903b9
   * @param newRules GrayReleaseRuleItemDTO{clientAppId=10001, clientIpList=[127.0.0.1, 127.0.0.11]}
   * @param recordReleaseHistory true
   * @param releaseOperation
   * 1、根据appId+clusterName+namespaceName+branchName 查找最新的灰度规则：GrayReleaseRule
   * 2、获得最新的灰度的子cluster 的 Release 对象的编号
   * 3、添加一条新的 GrayReleaseRule，并绑定最新的灰度的releaseId
   * 4、删除旧的GrayReleaseRule，使它的状态为删除状态
   * 5、判断是否记录历史记录，如果要的话，创建一条 ReleaseHistory
   */
  private void doUpdateBranchGrayRules(String appId, String clusterName, String namespaceName,
                                              String branchName, GrayReleaseRule newRules, boolean recordReleaseHistory, int releaseOperation) {
    // 获得子 Namespace 的灰度发布规则
    GrayReleaseRule oldRules = grayReleaseRuleRepository
        .findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);
    // 获得最新的子 Namespace 的 Release 对象
    Release latestBranchRelease = releaseService.findLatestActiveRelease(appId, branchName, namespaceName);
// 获得最新的子 Namespace 的 Release 对象的编号
    long latestBranchReleaseId = latestBranchRelease != null ? latestBranchRelease.getId() : 0;
    // 设置 GrayReleaseRule 的 `releaseId`
    newRules.setReleaseId(latestBranchReleaseId);
    // 保存新的 GrayReleaseRule 对象
    grayReleaseRuleRepository.save(newRules);
    // 删除老的 GrayReleaseRule 对象
    //delete old rules
    if (oldRules != null) {
      grayReleaseRuleRepository.delete(oldRules);
    }
    // 若需要，创建 ReleaseHistory 对象，并保存
    if (recordReleaseHistory) {
      Map<String, Object> releaseOperationContext = Maps.newHashMap();
      releaseOperationContext.put(ReleaseOperationContext.RULES, GrayReleaseRuleItemTransformer
          .batchTransformFromJSON(newRules.getRules()));
      if (oldRules != null) {
        releaseOperationContext.put(ReleaseOperationContext.OLD_RULES,
            GrayReleaseRuleItemTransformer.batchTransformFromJSON(oldRules.getRules()));
      }
      releaseHistoryService.createReleaseHistory(appId, clusterName, namespaceName, branchName, latestBranchReleaseId,
          latestBranchReleaseId, releaseOperation, releaseOperationContext, newRules.getDataChangeLastModifiedBy());
    }
  }

  /***
   * 更新GrayReleaseRule
   * @param appId appId：10001
   * @param clusterName 集群名称：default
   * @param namespaceName 命名空间名称：seata-properties
   * @param branchName 灰度分支子集群名称：20191207093742-e1d29b56c8e8a6fe
   * @param latestReleaseId
   * @param operator
   * @return
   * 1、查询最新的旧的 GrayReleaseRule
   * 2、新增一条新的 GrayReleaseRule
   * 3、将旧的 GrayReleaseRule 设置为失效
   */
  @Transactional
  public GrayReleaseRule updateRulesReleaseId(String appId, String clusterName,
                                   String namespaceName, String branchName,
                                   long latestReleaseId, String operator) {
    GrayReleaseRule oldRules = grayReleaseRuleRepository.
        findTopByAppIdAndClusterNameAndNamespaceNameAndBranchNameOrderByIdDesc(appId, clusterName, namespaceName, branchName);

    if (oldRules == null) {
      return null;
    }

    GrayReleaseRule newRules = new GrayReleaseRule();
    newRules.setBranchStatus(NamespaceBranchStatus.ACTIVE);
    newRules.setReleaseId(latestReleaseId);
    newRules.setRules(oldRules.getRules());
    newRules.setAppId(oldRules.getAppId());
    newRules.setClusterName(oldRules.getClusterName());
    newRules.setNamespaceName(oldRules.getNamespaceName());
    newRules.setBranchName(oldRules.getBranchName());
    newRules.setDataChangeCreatedBy(operator);
    newRules.setDataChangeLastModifiedBy(operator);

    grayReleaseRuleRepository.save(newRules);

    grayReleaseRuleRepository.delete(oldRules);

    return newRules;
  }

  @Transactional
  public void deleteBranch(String appId, String clusterName, String namespaceName,
                           String branchName, int branchStatus, String operator) {
    Cluster toDeleteCluster = clusterService.findOne(appId, branchName);
    if (toDeleteCluster == null) {
      return;
    }

    Release latestBranchRelease = releaseService.findLatestActiveRelease(appId, branchName, namespaceName);

    long latestBranchReleaseId = latestBranchRelease != null ? latestBranchRelease.getId() : 0;

    //update branch rules
    GrayReleaseRule deleteRule = new GrayReleaseRule();
    deleteRule.setRules("[]");
    deleteRule.setAppId(appId);
    deleteRule.setClusterName(clusterName);
    deleteRule.setNamespaceName(namespaceName);
    deleteRule.setBranchName(branchName);
    deleteRule.setBranchStatus(branchStatus);
    deleteRule.setDataChangeLastModifiedBy(operator);
    deleteRule.setDataChangeCreatedBy(operator);

    doUpdateBranchGrayRules(appId, clusterName, namespaceName, branchName, deleteRule, false, -1);

    //delete branch cluster
    clusterService.delete(toDeleteCluster.getId(), operator);

    int releaseOperation = branchStatus == NamespaceBranchStatus.MERGED ? ReleaseOperation
        .GRAY_RELEASE_DELETED_AFTER_MERGE : ReleaseOperation.ABANDON_GRAY_RELEASE;

    releaseHistoryService.createReleaseHistory(appId, clusterName, namespaceName, branchName, latestBranchReleaseId,
        latestBranchReleaseId, releaseOperation, null, operator);

    auditService.audit("Branch", toDeleteCluster.getId(), Audit.OP.DELETE, operator);
  }

  /***
   * 创建一个分支的集群
   * @param appId
   * @param parentCluster
   * @param namespaceName
   * @param operator
   * @return
   */
  private Cluster createChildCluster(String appId, Cluster parentCluster,
                                     String namespaceName, String operator) {

    Cluster childCluster = new Cluster();
    childCluster.setAppId(appId);
    childCluster.setParentClusterId(parentCluster.getId());//作为父子集群的关联
    //集群名称 随机的一个名称
    childCluster.setName(UniqueKeyGenerator.generate(appId, parentCluster.getName(), namespaceName));
    childCluster.setDataChangeCreatedBy(operator);
    childCluster.setDataChangeLastModifiedBy(operator);

    return childCluster;
  }

  /***
   * 创建分支的namespace
   * @param appId
   * @param clusterName 集群/子集群的名字
   * @param namespaceName
   * @param operator
   * @return
   */
  private Namespace createNamespaceBranch(String appId, String clusterName, String namespaceName, String operator) {
    Namespace childNamespace = new Namespace();
    childNamespace.setAppId(appId);
    childNamespace.setClusterName(clusterName);
    childNamespace.setNamespaceName(namespaceName);
    childNamespace.setDataChangeLastModifiedBy(operator);
    childNamespace.setDataChangeCreatedBy(operator);
    return childNamespace;
  }

}
