package com.ctrip.framework.apollo;

import com.ctrip.framework.apollo.core.enums.ConfigFileFormat;
import com.ctrip.framework.apollo.enums.ConfigSourceType;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface ConfigFile {
  /**
   * Get file content of the namespace
   * @return file content, {@code null} if there is no content
   * 获得配置文件的内容
   */
  String getContent();

  /**
   * Whether the config file has any content
   * @return true if it has content, false otherwise.
   * 是否包含配置信息
   */
  boolean hasContent();

  /**
   * Get the namespace of this config file instance
   * @return the namespace
   * 获得配置文件对应的 namespace
   */
  String getNamespace();

  /**
   * Get the file format of this config file instance
   * @return the config file format enum
   * 配置文件的格式：Properties("properties"), XML("xml"), JSON("json"), YML("yml"), YAML("yaml"), TXT("txt");
   */
  ConfigFileFormat getConfigFileFormat();

  /**
   * Add change listener to this config file instance.
   *
   * @param listener the config file change listener
   * 添加配置文件变化监听器
   */
  void addChangeListener(ConfigFileChangeListener listener);

  /**
   * Remove the change listener
   *
   * @param listener the specific config change listener to remove
   * @return true if the specific config change listener is found and removed
   */
  public boolean removeChangeListener(ConfigChangeListener listener);

  /**
   * Return the config's source type, i.e. where is the config loaded from
   *
   * @return the config's source type
   */
  public ConfigSourceType getSourceType();
}
