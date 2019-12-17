package com.ctrip.framework.apollo.portal.component;

import com.ctrip.framework.apollo.core.MetaDomainConsts;
import com.ctrip.framework.apollo.core.dto.ServiceDTO;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.core.utils.ApolloThreadFactory;
import com.ctrip.framework.apollo.tracer.Tracer;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.http.HttpMessageConverters;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/***
 * Admin Service 定位器
 */
@Component
public class AdminServiceAddressLocator {

  private static final long NORMAL_REFRESH_INTERVAL = 5 * 60 * 1000;
  private static final long OFFLINE_REFRESH_INTERVAL = 10 * 1000;
  private static final int RETRY_TIMES = 3;
  private static final String ADMIN_SERVICE_URL_PATH = "/services/admin";
  private static final Logger logger = LoggerFactory.getLogger(AdminServiceAddressLocator.class);

  private ScheduledExecutorService refreshServiceAddressService;
  private RestTemplate restTemplate;
  private List<Env> allEnvs;
  private Map<Env, List<ServiceDTO>> cache = new ConcurrentHashMap<>();

  private final PortalSettings portalSettings;
  private final RestTemplateFactory restTemplateFactory;

  public AdminServiceAddressLocator(
      final HttpMessageConverters httpMessageConverters,
      final PortalSettings portalSettings,
      final RestTemplateFactory restTemplateFactory) {
    this.portalSettings = portalSettings;
    this.restTemplateFactory = restTemplateFactory;
  }

  @PostConstruct
  public void init() {
    //获得所有的环境常量，默认是{FAT,UAT,PRO}
    allEnvs = portalSettings.getAllEnvs();

    //init restTemplate
    restTemplate = restTemplateFactory.getObject();
    // 创建 ScheduledExecutorService
    refreshServiceAddressService =
        Executors.newScheduledThreadPool(1, ApolloThreadFactory.create("ServiceLocator", true));
    // 创建延迟任务，1 秒后拉取 Admin Service 地址
    refreshServiceAddressService.schedule(new RefreshAdminServerAddressTask(), 1, TimeUnit.MILLISECONDS);
  }

  /***
   * 从缓存中获得服务列表，并排序后返回
   * @param env
   * @return
   * 1、从缓存中获得 ServiceDTO 列表，如果为空的，则直接返回
   * 2、对缓存中的services进行排序返回
   */
  public List<ServiceDTO> getServiceList(Env env) {
    // 从缓存中获得 ServiceDTO 列表，如果为空的，则直接返回
    List<ServiceDTO> services = cache.get(env);
    if (CollectionUtils.isEmpty(services)) {
      return Collections.emptyList();
    }
    //对缓存中的services进行排序返回
    List<ServiceDTO> randomConfigServices = Lists.newArrayList(services);
    Collections.shuffle(randomConfigServices);
    return randomConfigServices;
  }

  //maintain admin server address

  /***
   * 定时刷新AdminServer的任务
   * 1、遍历所有的环境Env
   * 2、依次根据环境变量env获取相应环境的最新的 adminserver地址列表
   * 3、如果只要有一个获取失败，则延迟10钟再去尝试获取刷新，否则每隔5分钟刷新依次
   */
  private class RefreshAdminServerAddressTask implements Runnable {

    @Override
    public void run() {
      boolean refreshSuccess = true;
      //refresh fail if get any env address fail
      for (Env env : allEnvs) {
        boolean currentEnvRefreshResult = refreshServerAddressCache(env);
        refreshSuccess = refreshSuccess && currentEnvRefreshResult;
      }

      if (refreshSuccess) {//如果所有的环境全程高，间隔5分钟刷新依次
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), NORMAL_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      } else {//如果只要有一个获取失败，则延迟10钟再去尝试获取刷新
        refreshServiceAddressService
            .schedule(new RefreshAdminServerAddressTask(), OFFLINE_REFRESH_INTERVAL, TimeUnit.MILLISECONDS);
      }
    }
  }

  /***
   * 根据不同的环境获取最新的admin server地址，并更新到缓存里
   * @param env
   * @return
   */
  private boolean refreshServerAddressCache(Env env) {

    for (int i = 0; i < RETRY_TIMES; i++) {

      try {
        // 请求 Meta Service ，获得 Admin Service 集群地址
        ServiceDTO[] services = getAdminServerAddress(env);
        // 获得结果为空，continue ，继续执行下一次请求
        if (services == null || services.length == 0) {
          continue;
        }
        // 更新缓存
        cache.put(env, Arrays.asList(services));
        return true;
      } catch (Throwable e) {
        logger.error(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                   env, MetaDomainConsts.getDomain(env)), e);
        Tracer
            .logError(String.format("Get admin server address from meta server failed. env: %s, meta server address:%s",
                                    env, MetaDomainConsts.getDomain(env)), e);
      }
    }
    return false;
  }

  /***
   * 从 meta server上获取
   * @param env
   * @return
   */
  private ServiceDTO[] getAdminServerAddress(Env env) {
    String domainName = MetaDomainConsts.getDomain(env);
    String url = domainName + ADMIN_SERVICE_URL_PATH;
    return restTemplate.getForObject(url, ServiceDTO[].class);
  }


}
