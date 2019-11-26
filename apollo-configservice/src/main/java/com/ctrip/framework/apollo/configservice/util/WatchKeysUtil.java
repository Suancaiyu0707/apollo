package com.ctrip.framework.apollo.configservice.util;

import com.ctrip.framework.apollo.common.entity.AppNamespace;
import com.ctrip.framework.apollo.configservice.service.AppNamespaceServiceWithCache;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
@Component
public class WatchKeysUtil {
  private static final Joiner STRING_JOINER = Joiner.on(ConfigConsts.CLUSTER_NAMESPACE_SEPARATOR);
  private final AppNamespaceServiceWithCache appNamespaceService;

  public WatchKeysUtil(final AppNamespaceServiceWithCache appNamespaceService) {
    this.appNamespaceService = appNamespaceService;
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespace, dataCenter combination
   */
  public Set<String> assembleAllWatchKeys(String appId, String clusterName, String namespace,
                                          String dataCenter) {
    Multimap<String, String> watchedKeysMap =
        assembleAllWatchKeys(appId, clusterName, Sets.newHashSet(namespace), dataCenter);
    return Sets.newHashSet(watchedKeysMap.get(namespace));
  }

  /**
   * Assemble watch keys for the given appId, cluster, namespaces, dataCenter combination
   *
   * @return a multimap with namespace as the key and watch keys as the value
   */
  /***
   *
   * @param appId  appId
   * @param clusterName 集群名称
   * @param namespaces namespace集合(一个客户端可能使用了多个namespace)
   * @param dataCenter 数据中心
   * @return
   * 1、获取监听的watchKey集合：{namespace,[appid+clustername+ namespace,appid+datacenter+ namespace]}
   */
  public Multimap<String, String> assembleAllWatchKeys(String appId, String clusterName,
                                                       Set<String> namespaces,
                                                       String dataCenter) {
    //获取监听的watchKey集合：{namespace,[appid+clustername+ namespace,appid+datacenter+ namespace]}
    Multimap<String, String> watchedKeysMap =
        assembleWatchKeys(appId, clusterName, namespaces, dataCenter);

    //Every app has an 'application' namespace
    if (!(
            namespaces.size() == 1
            && namespaces.contains(ConfigConsts.NAMESPACE_APPLICATION)//如果不只包含application的namespace，也就是除了application还有其它的
            )) {//校验namespace是否属于appId下面
      //根据appId和namespaces获取属于 appId的namespace列表
      Set<String> namespacesBelongToAppId = namespacesBelongToAppId(appId, namespaces);
      //获得public类型的namespace(不属于namespacesBelongToAppId下面的)
      Set<String> publicNamespaces = Sets.difference(namespaces, namespacesBelongToAppId);

      //如果存在public的Namespaces，则监听这些公共的namespace
      if (!publicNamespaces.isEmpty()) {
        watchedKeysMap
            .putAll(findPublicConfigWatchKeys(appId, clusterName, publicNamespaces, dataCenter));
      }
    }

    return watchedKeysMap;
  }

  /**
   *
   * @param applicationId
   * @param clusterName
   * @param namespaces
   * @param dataCenter
   * @return
   * 查找appId下的公共的namespace
   * 1、根据namespaces检索出公共的namespace列表
   * 2、从1中查找appid=applicationId的 namespace列表
   */
  private Multimap<String, String> findPublicConfigWatchKeys(String applicationId,
                                                             String clusterName,
                                                             Set<String> namespaces,
                                                             String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();
    //根据namespaces检索出公共的namespace列表
    List<AppNamespace> appNamespaces = appNamespaceService.findPublicNamespacesByNames(namespaces);

    for (AppNamespace appNamespace : appNamespaces) {
      //check whether the namespace's appId equals to current one
      if (Objects.equals(applicationId, appNamespace.getAppId())) {
        continue;
      }

      String publicConfigAppId = appNamespace.getAppId();

      watchedKeysMap.putAll(appNamespace.getName(),
          assembleWatchKeys(publicConfigAppId, clusterName, appNamespace.getName(), dataCenter));
    }

    return watchedKeysMap;
  }

  /**
   * 拼接watchkey
   * @param appId appid
   * @param cluster 集群名称
   * @param namespace 命名空间
   * @return
   * 返回 appId+cluster+namespace
   */
  private String assembleKey(String appId, String cluster, String namespace) {
    return STRING_JOINER.join(appId, cluster, namespace);
  }

  /***
   *
   * @param appId  appId
   * @param clusterName 集群名称
   * @param namespace namespace(耽搁)
   * @param dataCenter 数据中心
   * @return
   *  组装返回需要监听的key:
   *    appid+clustername+ namespace
   *    appid+datacenter+ namespace
   */
  private Set<String> assembleWatchKeys(String appId, String clusterName, String namespace,
                                        String dataCenter) {
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    Set<String> watchedKeys = Sets.newHashSet();

    //监听非默认default的集群的变化
    if (!Objects.equals(ConfigConsts.CLUSTER_NAME_DEFAULT, clusterName)) {
      //添加 appId+cluster+namespace
      watchedKeys.add(assembleKey(appId, clusterName, namespace));
    }

    //监听数据中心的消息的变化
    if (!Strings.isNullOrEmpty(dataCenter) && !Objects.equals(dataCenter, clusterName)) {
      //添加 appId+dataCenter+namespace
      watchedKeys.add(assembleKey(appId, dataCenter, namespace));
    }

    //监听默认的集群名称default的变化
    watchedKeys.add(assembleKey(appId, ConfigConsts.CLUSTER_NAME_DEFAULT, namespace));

    return watchedKeys;
  }

  /***
   *
   * @param appId  appId
   * @param clusterName 集群名称
   * @param namespaces namespace集合(一个客户端可能使用了多个namespace)
   * @param dataCenter 数据中心
   * @return
   * 1、遍历所有的 namespace
   * 2、根据 根据namespace和datacenter，维护监听的key
   *    {namespace,[appid+clustername+ namespace,appid+datacenter+ namespace]}
   */
  private Multimap<String, String> assembleWatchKeys(String appId, String clusterName,
                                                     Set<String> namespaces,
                                                     String dataCenter) {
    Multimap<String, String> watchedKeysMap = HashMultimap.create();
    //遍历namespace，
    for (String namespace : namespaces) {
      watchedKeysMap
          .putAll(namespace,
                  assembleWatchKeys(appId, clusterName, namespace, dataCenter)//返回set:[appid+clustername+ namespace,appid+datacenter+ namespace]
          );
    }

    return watchedKeysMap;
  }

  /***
   * 遍历namespaces，剔除不属于appId下的namespace
   * @param appId
   * @param namespaces
   * @return
   * 1、如果appid=ApolloNoAppIdPlaceHolder,返回空
   * 2、根据appId和namespaces获取属于 appId的AppNamespace列表的namespace
   */
  private Set<String> namespacesBelongToAppId(String appId, Set<String> namespaces) {
    //如果appid=
    if (ConfigConsts.NO_APPID_PLACEHOLDER.equalsIgnoreCase(appId)) {
      return Collections.emptySet();
    }
    List<AppNamespace> appNamespaces =
        appNamespaceService.findByAppIdAndNamespaces(appId, namespaces);

    if (appNamespaces == null || appNamespaces.isEmpty()) {
      return Collections.emptySet();
    }

    return appNamespaces.stream().map(AppNamespace::getName).collect(Collectors.toSet());
  }
}
