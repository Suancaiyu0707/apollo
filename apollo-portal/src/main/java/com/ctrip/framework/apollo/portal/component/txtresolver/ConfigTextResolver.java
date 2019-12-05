package com.ctrip.framework.apollo.portal.component.txtresolver;

import com.ctrip.framework.apollo.common.dto.ItemChangeSets;
import com.ctrip.framework.apollo.common.dto.ItemDTO;

import java.util.List;

/**
 * users can modify config in text mode.so need resolve text.
 */
public interface ConfigTextResolver {
  /**
   * 解析文本，创建 ItemChangeSets 对象
   *
   * @param namespaceId Namespace 编号
   * @param configText 配置文本
   * @param baseItems 已存在的 ItemDTO 们
   * @return ItemChangeSets 对象
   */
  ItemChangeSets resolve(long namespaceId, String configText, List<ItemDTO> baseItems);

}
