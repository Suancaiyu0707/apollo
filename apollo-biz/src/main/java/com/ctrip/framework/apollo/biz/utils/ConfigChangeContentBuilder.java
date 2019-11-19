package com.ctrip.framework.apollo.biz.utils;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.ctrip.framework.apollo.biz.entity.Item;
import com.ctrip.framework.apollo.core.utils.StringUtils;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import org.springframework.beans.BeanUtils;

/**
 * 配置变更内容构建器
 *
 * {
 *     "createItems": [ ],
 *     "updateItems": [
 *         {
 *             "oldItem": {
 *                 "namespaceId": 32,
 *                 "key": "key4",
 *                 "value": "value4123",
 *                 "comment": "123",
 *                 "lineNum": 4,
 *                 "id": 15,
 *                 "isDeleted": false,
 *                 "dataChangeCreatedBy": "apollo",
 *                 "dataChangeCreatedTime": "2018-04-27 16:49:59",
 *                 "dataChangeLastModifiedBy": "apollo",
 *                 "dataChangeLastModifiedTime": "2018-04-27 22:37:52"
 *             },
 *             "newItem": {
 *                 "namespaceId": 32,
 *                 "key": "key4",
 *                 "value": "value41234",
 *                 "comment": "123",
 *                 "lineNum": 4,
 *                 "id": 15,
 *                 "isDeleted": false,
 *                 "dataChangeCreatedBy": "apollo",
 *                 "dataChangeCreatedTime": "2018-04-27 16:49:59",
 *                 "dataChangeLastModifiedBy": "apollo",
 *                 "dataChangeLastModifiedTime": "2018-04-27 22:38:58"
 *             }
 *         }
 *     ],
 *     "deleteItems": [ ]
 * }
 */
public class ConfigChangeContentBuilder {

  private static final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd HH:mm:ss").create();
  //创建 Item 集合
  private List<Item> createItems = new LinkedList<>();
  //更新 Item 集合
  private List<ItemPair> updateItems = new LinkedList<>();
  //删除 Item 集合
  private List<Item> deleteItems = new LinkedList<>();


  public ConfigChangeContentBuilder createItem(Item item) {
    if (!StringUtils.isEmpty(item.getKey())){
      createItems.add(cloneItem(item));
    }
    return this;
  }

  public ConfigChangeContentBuilder updateItem(Item oldItem, Item newItem) {
    if (!oldItem.getValue().equals(newItem.getValue())){
      ItemPair itemPair = new ItemPair(cloneItem(oldItem), cloneItem(newItem));
      updateItems.add(itemPair);
    }
    return this;
  }

  public ConfigChangeContentBuilder deleteItem(Item item) {
    if (!StringUtils.isEmpty(item.getKey())) {
      deleteItems.add(cloneItem(item));
    }
    return this;
  }
  //判断是否有变化。当且仅当有变化才记录 Commit
  public boolean hasContent(){
    return !createItems.isEmpty() || !updateItems.isEmpty() || !deleteItems.isEmpty();
  }
  //构建 Item 变化的 JSON 字符串
  public String build() {
    //因为事务第一段提交并没有更新时间,所以build时统一更新
    Date now = new Date();

    for (Item item : createItems) {
      item.setDataChangeLastModifiedTime(now);
    }

    for (ItemPair item : updateItems) {
      item.newItem.setDataChangeLastModifiedTime(now);
    }

    for (Item item : deleteItems) {
      item.setDataChangeLastModifiedTime(now);
    }
    return gson.toJson(this);
  }

  static class ItemPair {
    //旧的item
    Item oldItem;
    //新的item
    Item newItem;

    public ItemPair(Item oldItem, Item newItem) {
      this.oldItem = oldItem;
      this.newItem = newItem;
    }
  }
  //调用 cloneItem(Item) 方法，克隆 Item 对象。因为在 #build() 方法中，会修改 Item 对象的属性
  Item cloneItem(Item source) {
    Item target = new Item();

    BeanUtils.copyProperties(source, target);

    return target;
  }

}
