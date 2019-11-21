package com.ctrip.framework.apollo.biz.message;

/**
 * @author Jason Song(song_s@ctrip.com)
 */
public interface MessageSender {
  /***
   * 发送消息
   * @param message 消息
   * @param channel 通道（主题）
   */
  void sendMessage(String message, String channel);
}
