package com.ctrip.framework.apollo.biz.repository;

import com.ctrip.framework.apollo.common.entity.AppNamespace;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.PagingAndSortingRepository;

import java.util.List;
import java.util.Set;

/***
 * 操作 ApolloConfigDB.AppNamespace 应用namespace定义表
 */
public interface AppNamespaceRepository extends PagingAndSortingRepository<AppNamespace, Long>{
  /***
   * 根据appid和namespace名称查询应用namespace
   * @param appId
   * @param namespaceName
   * @return
   */
  AppNamespace findByAppIdAndName(String appId, String namespaceName);

  /***
   * 根据appid和namespace名称查询应用namespaceNames
   * @param appId
   * @param namespaceNames
   * @return
   */
  List<AppNamespace> findByAppIdAndNameIn(String appId, Set<String> namespaceNames);

  /**
   *根据namespaceName查询公共的namespace
   * @param namespaceName
   * @return
   */
  AppNamespace findByNameAndIsPublicTrue(String namespaceName);

  /**
   * 根据namespaces查询公共的namespace：isPublic=true
   * @param namespaceNames
   * @return
   */
  List<AppNamespace> findByNameInAndIsPublicTrue(Set<String> namespaceNames);

  /**
   * 根据appId和isPublic来查询对应的 AppNamespace列表
   * @param appId
   * @param isPublic
   * @return
   */
  List<AppNamespace> findByAppIdAndIsPublic(String appId, boolean isPublic);

  List<AppNamespace> findByAppId(String appId);

  List<AppNamespace> findFirst500ByIdGreaterThanOrderByIdAsc(long id);

  /***
   * 根据 appid 删除所有的AppNamespace记录
   * @param appId
   * @param operator
   * @return
   */
  @Modifying
  @Query("UPDATE AppNamespace SET IsDeleted=1,DataChange_LastModifiedBy = ?2 WHERE AppId=?1")
  int batchDeleteByAppId(String appId, String operator);

  /**
   * 根据 appid和namespaceName删除所有的AppNamespace记录
   * @param appId
   * @param namespaceName
   * @param operator
   * @return
   */
  @Modifying
  @Query("UPDATE AppNamespace SET IsDeleted=1,DataChange_LastModifiedBy = ?3 WHERE AppId=?1 and Name = ?2")
  int delete(String appId, String namespaceName, String operator);
}
