package com.ctrip.framework.apollo.biz.service;

import com.ctrip.framework.apollo.biz.entity.Audit;
import com.ctrip.framework.apollo.biz.entity.Cluster;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.repository.AppNamespaceRepository;
import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.common.exception.ServiceException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.core.utils.StringUtils;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Service
public class AppNamespaceService {

  private static final Logger logger = LoggerFactory.getLogger(AppNamespaceService.class);

  private final AppNamespaceRepository appNamespaceRepository;
  private final NamespaceService namespaceService;
  private final ClusterService clusterService;
  private final AuditService auditService;

  public AppNamespaceService(
      final AppNamespaceRepository appNamespaceRepository,
      final @Lazy NamespaceService namespaceService,
      final @Lazy ClusterService clusterService,
      final AuditService auditService) {
    this.appNamespaceRepository = appNamespaceRepository;
    this.namespaceService = namespaceService;
    this.clusterService = clusterService;
    this.auditService = auditService;
  }

  /**
   * 根据appId和namespaceName 判断唯一性
   * @param appId
   * @param namespaceName
   * @return
   */
  public boolean isAppNamespaceNameUnique(String appId, String namespaceName) {
    Objects.requireNonNull(appId, "AppId must not be null");
    Objects.requireNonNull(namespaceName, "Namespace must not be null");
    return Objects.isNull(appNamespaceRepository.findByAppIdAndName(appId, namespaceName));
  }

  /**
   * 根据 namespaceName 查询公共的namespace
   * @param namespaceName
   * @return
   */
  public AppNamespace findPublicNamespaceByName(String namespaceName) {
    Preconditions.checkArgument(namespaceName != null, "Namespace must not be null");
    return appNamespaceRepository.findByNameAndIsPublicTrue(namespaceName);
  }

  /**
   * 根据appId查询所有的 namespace
   * @param appId
   * @return
   */
  public List<AppNamespace> findByAppId(String appId) {
    return appNamespaceRepository.findByAppId(appId);
  }
  /**
   * 根据namespaceNames查询所有的公共的 namespace
   * @param namespaceNames
   * @return
   */
  public List<AppNamespace> findPublicNamespacesByNames(Set<String> namespaceNames) {
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }

    return appNamespaceRepository.findByNameInAndIsPublicTrue(namespaceNames);
  }
  /**
   * 根据appId查询所有的私有的的 namespace
   * @param appId
   * @return
   */
  public List<AppNamespace> findPrivateAppNamespace(String appId) {
    return appNamespaceRepository.findByAppIdAndIsPublic(appId, false);
  }
  /**
   * 根据appId和namespaceName查询唯一 namespace
   * @param appId
   * @param namespaceName
   * @return
   */
  public AppNamespace findOne(String appId, String namespaceName) {
    Preconditions
        .checkArgument(!StringUtils.isContainEmpty(appId, namespaceName), "appId or Namespace must not be null");
    return appNamespaceRepository.findByAppIdAndName(appId, namespaceName);
  }
  /**
   * 根据appId和namespaceNames查询所有的 namespace
   * @param appId
   * @param namespaceNames
   * @return
   */
  public List<AppNamespace> findByAppIdAndNamespaces(String appId, Set<String> namespaceNames) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(appId), "appId must not be null");
    if (namespaceNames == null || namespaceNames.isEmpty()) {
      return Collections.emptyList();
    }
    return appNamespaceRepository.findByAppIdAndNameIn(appId, namespaceNames);
  }

  /**
   * 创建一个默认的 Namespace
   * @param appId 应用id
   * @param createBy 创建人
   */
  @Transactional
  public void createDefaultAppNamespace(String appId, String createBy) {
    if (!isAppNamespaceNameUnique(appId, ConfigConsts.NAMESPACE_APPLICATION)) {
      throw new ServiceException("appnamespace not unique");
    }
    AppNamespace appNs = new AppNamespace();
    appNs.setAppId(appId);
    appNs.setName(ConfigConsts.NAMESPACE_APPLICATION);//默认的namespace名称叫：application
    appNs.setComment("default app namespace");
    appNs.setFormat(ConfigFileFormat.Properties.getValue());
    appNs.setDataChangeCreatedBy(createBy);//创建人
    appNs.setDataChangeLastModifiedBy(createBy);
    appNamespaceRepository.save(appNs);
    //添加一条 创建 appNamespace的审计记录
    auditService.audit(AppNamespace.class.getSimpleName(), appNs.getId(), Audit.OP.INSERT,
                       createBy);
  }

  /**
   * 创建一个默认的 Namespace
   * @param appNamespace
   * @return
   */
  @Transactional
  public AppNamespace createAppNamespace(AppNamespace appNamespace) {
    String createBy = appNamespace.getDataChangeCreatedBy();
    //判断 appId下的命名空间唯一
    if (!isAppNamespaceNameUnique(appNamespace.getAppId(), appNamespace.getName())) {
      throw new ServiceException("appnamespace not unique");
    }
    appNamespace.setId(0);//protection
    appNamespace.setDataChangeCreatedBy(createBy);
    appNamespace.setDataChangeLastModifiedBy(createBy);
    //创建一条 AppNamespace
    appNamespace = appNamespaceRepository.save(appNamespace);
    // 创建 AppNamespace 在 App 下，每个 Cluster 的 Namespace 对象。
    createNamespaceForAppNamespaceInAllCluster(appNamespace.getAppId(), appNamespace.getName(), createBy);
    //添加一条 创建 appNamespace的审计记录
    auditService.audit(AppNamespace.class.getSimpleName(), appNamespace.getId(), Audit.OP.INSERT, createBy);
    return appNamespace;
  }

  public AppNamespace update(AppNamespace appNamespace) {
    AppNamespace managedNs = appNamespaceRepository.findByAppIdAndName(appNamespace.getAppId(), appNamespace.getName());
    BeanUtils.copyEntityProperties(appNamespace, managedNs);
    managedNs = appNamespaceRepository.save(managedNs);

    auditService.audit(AppNamespace.class.getSimpleName(), managedNs.getId(), Audit.OP.UPDATE,
                       managedNs.getDataChangeLastModifiedBy());

    return managedNs;
  }

  /***
   * 为每个集群信息维护一个 namespace 记录
   * namespace表中必须保证：appid、clusterName、namespaceName三者的唯一性
   * @param appId
   * @param namespaceName
   * @param createBy
   */
  public void createNamespaceForAppNamespaceInAllCluster(String appId, String namespaceName, String createBy) {
    //查询应用的所有集群信息
    List<Cluster> clusters = clusterService.findParentClusters(appId);

    for (Cluster cluster : clusters) {

      // in case there is some dirty data, e.g. public namespace deleted in other app and now created in this app
      if (!namespaceService.isNamespaceUnique(appId, cluster.getName(), namespaceName)) {
        continue;
      }

      Namespace namespace = new Namespace();
      namespace.setClusterName(cluster.getName());
      namespace.setAppId(appId);
      namespace.setNamespaceName(namespaceName);
      namespace.setDataChangeCreatedBy(createBy);
      namespace.setDataChangeLastModifiedBy(createBy);

      namespaceService.save(namespace);
    }
  }

  @Transactional
  public void batchDelete(String appId, String operator) {
    appNamespaceRepository.batchDeleteByAppId(appId, operator);
  }

  @Transactional
  public void deleteAppNamespace(AppNamespace appNamespace, String operator) {
    String appId = appNamespace.getAppId();
    String namespaceName = appNamespace.getName();

    logger.info("{} is deleting AppNamespace, appId: {}, namespace: {}", operator, appId, namespaceName);

    // 1. delete namespaces
    List<Namespace> namespaces = namespaceService.findByAppIdAndNamespaceName(appId, namespaceName);

    if (namespaces != null) {
      for (Namespace namespace : namespaces) {
        namespaceService.deleteNamespace(namespace, operator);
      }
    }

    // 2. delete app namespace
    appNamespaceRepository.delete(appId, namespaceName, operator);
  }
}
