package com.ctrip.framework.apollo;

import com.ctrip.framework.apollo.model.ConfigFileChangeEvent;

/**
 * @author Jason Song(song_s@ctrip.com)
 * 配置文件变化监听器
 */
public interface ConfigFileChangeListener {
  /**
   * Invoked when there is any config change for the namespace.
   * @param changeEvent the event for this change
   * 当配置文件内容发生变化，则回调该方法
   */
  void onChange(ConfigFileChangeEvent changeEvent);
}
