package com.ctrip.framework.apollo.configservice.service.config;

import com.ctrip.framework.apollo.biz.entity.Release;
import com.ctrip.framework.apollo.biz.grayReleaseRule.GrayReleaseRulesHolder;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.dto.ApolloNotificationMessages;

import com.google.common.base.Strings;

import java.util.Objects;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public abstract class AbstractConfigService implements ConfigService {
  @Autowired
  private GrayReleaseRulesHolder grayReleaseRulesHolder;

  /***
   *  根据clientMessages查询对应的发布版本记录
   * @param clientAppId 客户端的appId
   * @param clientIp 客户端ip （因为如果是灰度发布，就要根据ip去匹配）
   * @param configAppId apollo上配置的appId
   * @param configClusterName apollo上配置的clusterName
   * @param configNamespace apollo上配置的namespace
   * @param dataCenter apollo上配置的数据中心
   * @param clientMessages 客户端通过监听接收到的变更的消息
   * @return
   * 1、判断是否是默认的集群：default
   *    如果不是默认的集群：先查询灰度的发布记录(灰度的发布记录要根据clientIp进行过滤，因为灰度是针对部分ip进行发布)。
   *        如果存在灰度发布记录，则直接返回灰度发布版本。
   *        如果不存在灰度发布记录，则查询正式版本最新的一条发布记录。
   * 2、入根据集群名称和clientMessages查询不到最新的发布版本记录，则根据dataCenter和clientMessages查询最新的发布版本。
   *        如果存在灰度发布记录，则直接返回灰度发布版本。
   *        如果不存在灰度发布记录，则查询正式版本最新的一条发布记录。
   * 3、根据默认的集群名default和clientMessages查询发布正式发布版本记录
   *        如果存在灰度发布记录，则直接返回灰度发布版本。
   *        如果不存在灰度发布记录，则查询正式版本最新的一条发布记录。
   */
  @Override
  public Release loadConfig(String clientAppId, String clientIp, String configAppId, String configClusterName,
      String configNamespace, String dataCenter, ApolloNotificationMessages clientMessages) {
    // 如果不是默认的集群：default
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, configClusterName)) {
      Release clusterRelease = findRelease(clientAppId, clientIp, configAppId, configClusterName, configNamespace,
          clientMessages);

      if (!Objects.isNull(clusterRelease)) {
        return clusterRelease;
      }
    }

    // try to load via data center
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, configClusterName)) {
      Release dataCenterRelease = findRelease(clientAppId, clientIp, configAppId, dataCenter, configNamespace,
          clientMessages);
      if (!Objects.isNull(dataCenterRelease)) {
        return dataCenterRelease;
      }
    }

    // fallback to default release
    return findRelease(clientAppId, clientIp, configAppId, ConfigConsts.CLUSTER_NAME_DEFAULT, configNamespace,
        clientMessages);
  }

  /***
   *  查询发布版本
   * @param clientAppId 客户端的appId
   * @param clientIp 客户端ip
   * @param configAppId apollo上配置的appId
   * @param configClusterName apollo上配置的clusterName
   * @param configNamespace apollo上配置的namespace
   * @param clientMessages 客户端通过监听接收到的变更的消息
   * @return
   * 因为对于灰度发布的时候，它只会配置只对部分的ip生效。所以这边要先根据clientIp查询灰度发布记录。
   *    如果存在再根据grayRelease表中的ReleaseId查询发布记录Release表。
   *    如果没有灰度发布记录，则直接查询最新的一条Release记录
   */
  private Release findRelease(String clientAppId, String clientIp, String configAppId, String configClusterName,
      String configNamespace, ApolloNotificationMessages clientMessages) {
    //先查找灰度发布记录，并返回灰度发布的版本id,映射的是release表的中id
    Long grayReleaseId = grayReleaseRulesHolder.findReleaseIdFromGrayReleaseRule(clientAppId, clientIp, configAppId,
        configClusterName, configNamespace);

    Release release = null;
    //如果灰度
    if (grayReleaseId != null) {
      //根据grayReleaseId从Release表中查询记录
      release = findActiveOne(grayReleaseId, clientMessages);
    }
    //如果release==null，表示没有灰度发布记录，则查询最新的一条Release记录
    if (release == null) {
      release = findLatestActiveRelease(configAppId, configClusterName, configNamespace, clientMessages);
    }

    return release;
  }

  /**
   * Find active release by id
   */
  protected abstract Release findActiveOne(long id, ApolloNotificationMessages clientMessages);

  /**
   * Find active release by app id, cluster name and namespace name
   */
  protected abstract Release findLatestActiveRelease(String configAppId, String configClusterName,
      String configNamespaceName, ApolloNotificationMessages clientMessages);
}
