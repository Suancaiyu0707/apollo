package com.ctrip.framework.apollo.adminservice.controller;

import com.ctrip.framework.apollo.adminservice.aop.PreAcquireNamespaceLock;
import com.ctrip.framework.apollo.biz.entity.Commit;
import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.biz.entity.Namespace;
import com.ctrip.framework.apollo.biz.service.CommitService;
import com.ctrip.framework.apollo.biz.service.ItemService;
import com.ctrip.framework.apollo.biz.service.NamespaceService;
import com.ctrip.framework.apollo.biz.utils.ConfigChangeContentBuilder;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.exception.NotFoundException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
public class ItemController {

  private final ItemService itemService;
  private final NamespaceService namespaceService;
  private final CommitService commitService;

  public ItemController(final ItemService itemService, final NamespaceService namespaceService, final CommitService commitService) {
    this.itemService = itemService;
    this.namespaceService = namespaceService;
    this.commitService = commitService;
  }

  /***
   * 创建一个item
   * @param appId 应用id
   * @param clusterName 集群名称
   * @param namespaceName namespace名称
   * @param dto
   * @return
   * 1、利用构建器ConfigChangeContentBuilder构建一个包含了item变更的信息的构建器
   * 2、创建一个新的item对象
   * 3、维护一条Commit操作记录
   */
  @PreAcquireNamespaceLock
  @PostMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/items")
  public ItemDTO create(@PathVariable("appId") String appId,
                        @PathVariable("clusterName") String clusterName,
                        @PathVariable("namespaceName") String namespaceName, @RequestBody ItemDTO dto) {
    Item entity = BeanUtils.transform(Item.class, dto);
  //创建 ConfigChangeContentBuilder 对象
    ConfigChangeContentBuilder builder = new ConfigChangeContentBuilder();
    //校验对应的 Item 是否已经存在。若是，抛出 BadRequestException 异常
    Item managedEntity = itemService.findOne(appId, clusterName, namespaceName, entity.getKey());
    if (managedEntity != null) {
      throw new BadRequestException("item already exists");
    } else {
      // 保存 Item 对象
      entity = itemService.save(entity);
      // 添加到 ConfigChangeContentBuilder 中
      builder.createItem(entity);
    }
    dto = BeanUtils.transform(ItemDTO.class, entity);

    Commit commit = new Commit();
    commit.setAppId(appId);
    commit.setClusterName(clusterName);
    commit.setNamespaceName(namespaceName);
    commit.setChangeSets(builder.build());
    commit.setDataChangeCreatedBy(dto.getDataChangeLastModifiedBy());
    commit.setDataChangeLastModifiedBy(dto.getDataChangeLastModifiedBy());
    // 保存 Commit 对象
    commitService.save(commit);

    return dto;
  }

  @PreAcquireNamespaceLock
  @PutMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/items/{itemId}")
  public ItemDTO update(@PathVariable("appId") String appId,
                        @PathVariable("clusterName") String clusterName,
                        @PathVariable("namespaceName") String namespaceName,
                        @PathVariable("itemId") long itemId,
                        @RequestBody ItemDTO itemDTO) {

    Item entity = BeanUtils.transform(Item.class, itemDTO);

    ConfigChangeContentBuilder builder = new ConfigChangeContentBuilder();

    Item managedEntity = itemService.findOne(itemId);
    if (managedEntity == null) {
      throw new BadRequestException("item not exist");
    }

    Item beforeUpdateItem = BeanUtils.transform(Item.class, managedEntity);

    //protect. only value,comment,lastModifiedBy can be modified
    managedEntity.setValue(entity.getValue());
    managedEntity.setComment(entity.getComment());
    managedEntity.setDataChangeLastModifiedBy(entity.getDataChangeLastModifiedBy());

    entity = itemService.update(managedEntity);
    builder.updateItem(beforeUpdateItem, entity);
    itemDTO = BeanUtils.transform(ItemDTO.class, entity);

    if (builder.hasContent()) {
      Commit commit = new Commit();
      commit.setAppId(appId);
      commit.setClusterName(clusterName);
      commit.setNamespaceName(namespaceName);
      commit.setChangeSets(builder.build());
      commit.setDataChangeCreatedBy(itemDTO.getDataChangeLastModifiedBy());
      commit.setDataChangeLastModifiedBy(itemDTO.getDataChangeLastModifiedBy());
      commitService.save(commit);
    }

    return itemDTO;
  }

  @PreAcquireNamespaceLock
  @DeleteMapping("/items/{itemId}")
  public void delete(@PathVariable("itemId") long itemId, @RequestParam String operator) {
    Item entity = itemService.findOne(itemId);
    if (entity == null) {
      throw new NotFoundException("item not found for itemId " + itemId);
    }
    itemService.delete(entity.getId(), operator);

    Namespace namespace = namespaceService.findOne(entity.getNamespaceId());

    Commit commit = new Commit();
    commit.setAppId(namespace.getAppId());
    commit.setClusterName(namespace.getClusterName());
    commit.setNamespaceName(namespace.getNamespaceName());
    commit.setChangeSets(new ConfigChangeContentBuilder().deleteItem(entity).build());
    commit.setDataChangeCreatedBy(operator);
    commit.setDataChangeLastModifiedBy(operator);
    commitService.save(commit);
  }

  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/items")
  public List<ItemDTO> findItems(@PathVariable("appId") String appId,
                                 @PathVariable("clusterName") String clusterName,
                                 @PathVariable("namespaceName") String namespaceName) {
    return BeanUtils.batchTransform(ItemDTO.class, itemService.findItemsWithOrdered(appId, clusterName, namespaceName));
  }

  @GetMapping("/items/{itemId}")
  public ItemDTO get(@PathVariable("itemId") long itemId) {
    Item item = itemService.findOne(itemId);
    if (item == null) {
      throw new NotFoundException("item not found for itemId " + itemId);
    }
    return BeanUtils.transform(ItemDTO.class, item);
  }

  /***
   * 查询item是否已存在
   * @param appId appId
   * @param clusterName 集群名称
   * @param namespaceName 命名空间名称
   * @param key item的key
   * @return
   * 1、先根据appId, clusterName, namespaceName查询到namespace
   * 2、再根据namespaceId和key查询item
   */
  @GetMapping("/apps/{appId}/clusters/{clusterName}/namespaces/{namespaceName}/items/{key:.+}")
  public ItemDTO get(@PathVariable("appId") String appId,
                     @PathVariable("clusterName") String clusterName,
                     @PathVariable("namespaceName") String namespaceName, @PathVariable("key") String key) {
    Item item = itemService.findOne(appId, clusterName, namespaceName, key);
    if (item == null) {
      throw new NotFoundException(
          String.format("item not found for %s %s %s %s", appId, clusterName, namespaceName, key));
    }
    return BeanUtils.transform(ItemDTO.class, item);
  }


}
