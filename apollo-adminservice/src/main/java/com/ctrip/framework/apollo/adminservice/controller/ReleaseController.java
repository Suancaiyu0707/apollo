package com.ctrip.framework.apollo.adminservice.controller;


import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.message.MessageSender;
import com.ctrip.framework.apollo.biz.message.Topics;
import com.ctrip.framework.apollo.biz.service.NamespaceBranchService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.service.ReleaseService;
import com.ctrip.framework.apollo.biz.utils.ReleaseMessageKeyGenerator;
import com.ctrip.framework.apollo.common.constants.NamespaceBranchStatus;
import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ReleaseDTO;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import com.google.common.base.Splitter;
import org.springframework.data.domain.Pageable;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@RestController
public class ReleaseController {

  private static final Splitter RELEASES_SPLITTER = Splitter.on(",").omitEmptyStrings()
      .trimResults();

  private final ReleaseService releaseService;
  private final NamespaceService namespaceService;
  private final MessageSender messageSender;
  private final NamespaceBranchService namespaceBranchService;

  public ReleaseController(
      final ReleaseService releaseService,
      final NamespaceService namespaceService,
      final MessageSender messageSender,
      final NamespaceBranchService namespaceBranchService) {
    this.releaseService = releaseService;
    this.namespaceService = namespaceService;
    this.messageSender = messageSender;
    this.namespaceBranchService = namespaceBranchService;
  }


  @GetMapping("/releases/{releaseId}")
  public ReleaseDTO get(@PathVariable("releaseId") long releaseId) {
    Release release = releaseService.findOne(releaseId);
    if (release == null) {
      throw new NotFoundException(String.format("release not found for %s", releaseId));
    }
    return BeanUtils.transform(ReleaseDTO.class, release);
  }

  /***
   *
   * @param releaseIds
   * @return
   */
  @GetMapping("/releases")
  public List<ReleaseDTO> findReleaseByIds(@RequestParam("releaseIds") String releaseIds) {
    Set<Long> releaseIdSet = RELEASES_SPLITTER.splitToList(releaseIds).stream().map(Long::parseLong)
        .collect(Collectors.toSet());

    List<Release> releases = releaseService.findByReleaseIds(releaseIdSet);

    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/all")
  public List<ReleaseDTO> findAllReleases(@PathVariable("appId") String appId,
                                          @PathVariable("clusterName") String clusterName,
                                          @PathVariable("namespaceName") String namespaceName,
                                          Pageable page) {
    List<Release> releases = releaseService.findAllReleases(appId, clusterName, namespaceName, page);
    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }


  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/active")
  public List<ReleaseDTO> findActiveReleases(@PathVariable("appId") String appId,
                                             @PathVariable("clusterName") String clusterName,
                                             @PathVariable("namespaceName") String namespaceName,
                                             Pageable page) {
    List<Release> releases = releaseService.findActiveReleases(appId, clusterName, namespaceName, page);
    return BeanUtils.batchTransform(ReleaseDTO.class, releases);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases/latest")
  public ReleaseDTO getLatest(@PathVariable("appId") String appId,
                              @PathVariable("clusterName") String clusterName,
                              @PathVariable("namespaceName") String namespaceName) {
    Release release = releaseService.findLatestActiveRelease(appId, clusterName, namespaceName);
    return BeanUtils.transform(ReleaseDTO.class, release);
  }

  /***
   * 由portal模块调过来的发布请求
   * @param appId appId
   * @param clusterName 集群名称
   * @param namespaceName 命名空间
   * @param releaseName 发布标题
   * @param releaseComment 发布描述
   * @param operator 操作人
   * @param isEmergencyPublish 是否紧急发布
   * @return
   * 1、根据appId、clusterName、namespacename查询对应的要发布的namespace
   * 2、调用releaseService发布配置
   * 3、如果是一个灰度发布的话，查找主干的clusterName
   * 4、向客户端发送发布的消息，通知客户端拉取最新的配置
   */
  @Transactional
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/releases")
  public ReleaseDTO publish(@PathVariable("appId") String appId,
                            @PathVariable("clusterName") String clusterName,
                            @PathVariable("namespaceName") String namespaceName,
                            @RequestParam("name") String releaseName,
                            @RequestParam(name = "comment", required = false) String releaseComment,
                            @RequestParam("operator") String operator,
                            @RequestParam(name = "isEmergencyPublish", defaultValue = "false") boolean isEmergencyPublish) {
    // 校验对应的 Namespace 对象是否存在。若不存在，抛出 NotFoundException 异常
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException(String.format("Could not find namespace for %s %s %s", appId,
                                                clusterName, namespaceName));
    }
    // 发布 Namespace 的配置
    Release release = releaseService.publish(namespace, releaseName, releaseComment, operator, isEmergencyPublish);

    //send release message
    //根据namespace命名空间获得cluster集群名称
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);
    String messageCluster;
    /**
     * 若有父 Namespace 对象，说明是子 Namespace ( 灰度发布 )，则使用父 Namespace 的 Cluster 名字。
     * 因为，客户端即使在灰度发布的情况下，也是使用 父 Namespace 的 Cluster 名字。也就说，灰度发布，对客户端是透明无感知的。
     */
    if (parentNamespace != null) {// 表示是灰度发布
      messageCluster = parentNamespace.getClusterName();
    } else {// 使用请求的 ClusterName
      messageCluster = clusterName;
    }
    //发送 Release 消息 通知客户端
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, messageCluster, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
    return BeanUtils.transform(ReleaseDTO.class, release);
  }


  /**
   * merge branch items to master and publish master
   *
   * @return published result
   */
  /***
   * 更新发布
   * @param appId 应用id
   * @param clusterName 集群名称
   * @param namespaceName 命名空间
   * @param releaseName 发布标题
   * @param branchName
   * @param deleteBranch
   * @param releaseComment
   * @param isEmergencyPublish
   * @param changeSets
   * @return
   */
  @Transactional
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/updateAndPublish")
  public ReleaseDTO updateAndPublish(@PathVariable("appId") String appId,
                                     @PathVariable("clusterName") String clusterName,
                                     @PathVariable("namespaceName") String namespaceName,
                                     @RequestParam("releaseName") String releaseName,
                                     @RequestParam("branchName") String branchName,
                                     @RequestParam(value = "deleteBranch", defaultValue = "true") boolean deleteBranch,
                                     @RequestParam(name = "releaseComment", required = false) String releaseComment,
                                     @RequestParam(name = "isEmergencyPublish", defaultValue = "false") boolean isEmergencyPublish,
                                     @RequestBody ItemChangeSets changeSets) {
    /***
     * 检查命名空间是否存在
     */
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException(String.format("Could not find namespace for %s %s %s", appId,
                                                clusterName, namespaceName));
    }
    /***
     * 配置 发布
     */
    Release release = releaseService.mergeBranchChangeSetsAndRelease(namespace, branchName, releaseName,
                                                                     releaseComment, isEmergencyPublish, changeSets);

    if (deleteBranch) {
      namespaceBranchService.deleteBranch(appId, clusterName, namespaceName, branchName,
                                          NamespaceBranchStatus.MERGED, changeSets.getDataChangeLastModifiedBy());
    }
    /***
     * 往ReleaseMessage表插入一条消息记录
     *  消息内容：AppId+Cluster+Namespace
     */
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);

    return BeanUtils.transform(ReleaseDTO.class, release);

  }

  @Transactional
  @PutMapping("/releases/{releaseId}/rollback")
  public void rollback(@PathVariable("releaseId") long releaseId,
                       @RequestParam("operator") String operator) {

    Release release = releaseService.rollback(releaseId, operator);

    String appId = release.getAppId();
    String clusterName = release.getClusterName();
    String namespaceName = release.getNamespaceName();
    //send release message
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, clusterName, namespaceName),
                              Topics.APOLLO_RELEASE_TOPIC);
  }

  @Transactional
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/gray-del-releases")
  public ReleaseDTO publish(@PathVariable("appId") String appId,
                            @PathVariable("clusterName") String clusterName,
                            @PathVariable("namespaceName") String namespaceName,
                            @RequestParam("operator") String operator,
                            @RequestParam("releaseName") String releaseName,
                            @RequestParam(name = "comment", required = false) String releaseComment,
                            @RequestParam(name = "isEmergencyPublish", defaultValue = "false") boolean isEmergencyPublish,
                            @RequestParam(name = "grayDelKeys") Set<String> grayDelKeys){
    Namespace namespace = namespaceService.findOne(appId, clusterName, namespaceName);
    if (namespace == null) {
      throw new NotFoundException(String.format("Could not find namespace for %s %s %s", appId,
              clusterName, namespaceName));
    }

    Release release = releaseService.grayDeletionPublish(namespace, releaseName, releaseComment, operator, isEmergencyPublish, grayDelKeys);

    //send release message
    Namespace parentNamespace = namespaceService.findParentNamespace(namespace);
    String messageCluster;
    if (parentNamespace != null) {
      messageCluster = parentNamespace.getClusterName();
    } else {
      messageCluster = clusterName;
    }
    messageSender.sendMessage(ReleaseMessageKeyGenerator.generate(appId, messageCluster, namespaceName),
            Topics.APOLLO_RELEASE_TOPIC);
    return BeanUtils.transform(ReleaseDTO.class, release);
  }

}
