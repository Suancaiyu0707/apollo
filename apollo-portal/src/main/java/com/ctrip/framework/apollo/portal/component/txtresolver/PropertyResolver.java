package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.utils.BeanUtils;

import com.google.common.base.Strings;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * normal property file resolver.
 * update comment and blank item implement by create new item and delete old item.
 * update normal key/value item implement by update.
 * properties 配置解析器
 */
@Component("propertyResolver")
public class PropertyResolver implements ConfigTextResolver {

  private static final String KV_SEPARATOR = "=";
  private static final String ITEM_SEPARATOR = "\n";

  /***
   *
   * @param namespaceId Namespace 编号
   * @param configText 配置文本
   * @param baseItems 已存在的 ItemDTO 们
   * @return
   */
  @Override
  public ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems) {
    // 创建 Item Map ，以 lineNum 为 键
    Map<Integer, ItemDTO> oldLineNumMapItem = BeanUtils.mapByKey("lineNum", baseItems);
    // 创建 Item Map ，以 key 为 键
    Map<String, ItemDTO> oldKeyMapItem = BeanUtils.mapByKey("key", baseItems);

    //remove comment and blank item map.
    oldKeyMapItem.remove("");
    // 按照拆分 新的 Property 配置文件
    String[] newItems = configText.split(ITEM_SEPARATOR);
    // 校验是否存在重复配置 Key 。若是，抛出 BadRequestException 异常
    if (isHasRepeatKey(newItems)) {
      throw new BadRequestException("config text has repeat key please check.");
    }
    // 创建 ItemChangeSets 对象，并解析配置文件到 ItemChangeSets 中。
    ItemChangeSets changeSets = new ItemChangeSets();
    Map<Integer, String> newLineNumMapItem = new HashMap<>();//use for delete blank and comment item
    int lineCounter = 1;//行号
    //遍历新的item
    for (String newItem : newItems) {
      newItem = newItem.trim();
      newLineNumMapItem.put(lineCounter, newItem);
      // 使用行号，获得已存在的对应 ItemDTO
      ItemDTO oldItemByLine = oldLineNumMapItem.get(lineCounter);

      //comment item
      // comment item 注释 Item,也就是是否已#或!开头
      if (isCommentItem(newItem)) {//如果新的item以#或!开头，则表示注释行
        //如果oldItemByLine也是一个注释行且新旧item没有变，则不创建新的item，不然就创建新的item
        handleCommentLine(namespaceId, oldItemByLine, newItem, lineCounter, changeSets);
      } else if (isBlankItem(newItem)) {//如果新的item是个空白item
        //如果旧的item对应的不是空行，则新建一个item
        handleBlankLine(namespaceId, oldItemByLine, lineCounter, changeSets);

        //normal item
      } else { // 如果新的item是个普通item
        //处理是创建还是更新
        handleNormalLine(namespaceId, oldKeyMapItem, newItem, lineCounter, changeSets);
      }

      lineCounter++;
    }
    // 删除注释和空行配置项
    deleteCommentAndBlankItem(oldLineNumMapItem, newLineNumMapItem, changeSets);
    // 删除普通配置项
    deleteNormalKVItem(oldKeyMapItem, changeSets);

    return changeSets;
  }

  private boolean isHasRepeatKey(String[] newItems) {
    Set<String> keys = new HashSet<>();
    int lineCounter = 1;
    int keyCount = 0;
    for (String item : newItems) {
      if (!isCommentItem(item) && !isBlankItem(item)) {
        keyCount++;
        String[] kv = parseKeyValueFromItem(item);
        if (kv != null) {
          keys.add(kv[0].toLowerCase());
        } else {
          throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
        }
      }
      lineCounter++;
    }

    return keyCount > keys.size();
  }

  /***
   * 从item中解析出key，'='号前的
   * @param item
   * @return
   */
  private String[] parseKeyValueFromItem(String item) {
    int kvSeparator = item.indexOf(KV_SEPARATOR);
    if (kvSeparator == -1) {
      return null;
    }

    String[] kv = new String[2];
    kv[0] = item.substring(0, kvSeparator).trim();
    kv[1] = item.substring(kvSeparator + 1, item.length()).trim();
    return kv;
  }

  private void handleCommentLine(Long namespaceId, ItemDTO oldItemByLine, String newItem, int lineCounter, ItemChangeSets changeSets) {
    String oldComment = oldItemByLine == null ? "" : oldItemByLine.getComment();
    //create comment. implement update comment by delete old comment and create new comment
    // 创建注释 ItemDTO 到 ItemChangeSets 的新增项，若老的配置项不是注释或者不相等。另外，更新注释配置，通过删除 + 添加的方式。
    if (!(isCommentItem(oldItemByLine) && newItem.equals(oldComment))) {
      changeSets.addCreateItem(buildCommentItem(0l, namespaceId, newItem, lineCounter));
    }
  }

  /**
   * 如果旧的item对应的不是空行，则新建一个item
   * @param namespaceId
   * @param oldItem
   * @param lineCounter
   * @param changeSets
   */
  private void handleBlankLine(Long namespaceId, ItemDTO oldItem, int lineCounter, ItemChangeSets changeSets) {
    // 创建空行 ItemDTO 到 ItemChangeSets 的新增项，若老的不是空行。另外，更新空行配置，通过删除 + 添加的方式
    if (!isBlankItem(oldItem)) {
      changeSets.addCreateItem(buildBlankItem(0l, namespaceId, lineCounter));
    }
  }

  /***
   *
   * @param namespaceId
   * @param keyMapOldItem 旧的item列表，key是item的key
   * @param newItem 普通item
   * @param lineCounter 行号
   * @param changeSets
   */
  private void handleNormalLine(Long namespaceId, Map<String, ItemDTO> keyMapOldItem, String newItem,
                                int lineCounter, ItemChangeSets changeSets) {
    //从新的item里解析一行，生成 [key, value]
    String[] kv = parseKeyValueFromItem(newItem);

    if (kv == null) {
      throw new BadRequestException("line:" + lineCounter + " key value must separate by '='");
    }

    String newKey = kv[0];//'='号前的key
    //'='号后的valu
    String newValue = kv[1].replace("\\n", "\n"); //handle user input \n
    // 获得老的 ItemDTO 对象
    ItemDTO oldItem = keyMapOldItem.get(newKey);
    // 不存在，则创建 ItemDTO 到 ItemChangeSets 的添加项
    if (oldItem == null) {//new item
      changeSets.addCreateItem(buildNormalItem(0l, namespaceId, newKey, newValue, "", lineCounter));
    } else if (!newValue.equals(oldItem.getValue()) || lineCounter != oldItem.getLineNum()) {//如果值或者行号不相等，则创建 ItemDTO 到 ItemChangeSets 的修改项
      changeSets.addUpdateItem(
          buildNormalItem(oldItem.getId(), namespaceId, newKey, newValue, oldItem.getComment(),
              lineCounter));
    }
    // 移除老的 ItemDTO 对象
    keyMapOldItem.remove(newKey);
  }

  private boolean isCommentItem(ItemDTO item) {
    return item != null && "".equals(item.getKey())
        && (item.getComment().startsWith("#") || item.getComment().startsWith("!"));
  }

  private boolean isCommentItem(String line) {
    return line != null && (line.startsWith("#") || line.startsWith("!"));
  }

  private boolean isBlankItem(ItemDTO item) {
    return item != null && "".equals(item.getKey()) && "".equals(item.getComment());
  }

  private boolean isBlankItem(String line) {
    return  Strings.nullToEmpty(line).trim().isEmpty();
  }

  /**
   * 将剩余的配置项，添加到 ItemChangeSets 的删除项
   * @param baseKeyMapItem
   * @param changeSets
   */
  private void deleteNormalKVItem(Map<String, ItemDTO> baseKeyMapItem, ItemChangeSets changeSets) {
    //surplus item is to be deleted
    for (Map.Entry<String, ItemDTO> entry : baseKeyMapItem.entrySet()) {
      changeSets.addDeleteItem(entry.getValue());
    }
  }

  /**
   * 删除注释和空行配置项
   * @param oldLineNumMapItem
   * @param newLineNumMapItem
   * @param changeSets
   */
  private void deleteCommentAndBlankItem(Map<Integer, ItemDTO> oldLineNumMapItem,
                                         Map<Integer, String> newLineNumMapItem,
                                         ItemChangeSets changeSets) {

    for (Map.Entry<Integer, ItemDTO> entry : oldLineNumMapItem.entrySet()) {
      int lineNum = entry.getKey();
      ItemDTO oldItem = entry.getValue();
      String newItem = newLineNumMapItem.get(lineNum);
      // 添加到 ItemChangeSets 的删除项
      //1. old is blank by now is not
      //2.old is comment by now is not exist or modified
      if ((isBlankItem(oldItem) && !isBlankItem(newItem))
          || isCommentItem(oldItem) && (newItem == null || !newItem.equals(oldItem.getComment()))) {
        changeSets.addDeleteItem(oldItem);
      }
    }
  }

  private ItemDTO buildCommentItem(Long id, Long namespaceId, String comment, int lineNum) {
    return buildNormalItem(id, namespaceId, "", "", comment, lineNum);
  }

  private ItemDTO buildBlankItem(Long id, Long namespaceId, int lineNum) {
    return buildNormalItem(id, namespaceId, "", "", "", lineNum);
  }

  private ItemDTO buildNormalItem(Long id, Long namespaceId, String key, String value, String comment, int lineNum) {
    ItemDTO item = new ItemDTO(key, value, comment, lineNum);
    item.setId(id);
    item.setNamespaceId(namespaceId);
    return item;
  }
}
