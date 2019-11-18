package com.ctrip.framework.apollo.portal.controller;


import com.ctrip.framework.apollo.common.entity.App;
import com.ctrip.framework.apollo.common.exception.BadRequestException;
import com.ctrip.framework.apollo.common.http.MultiResponseEntity;
import com.ctrip.framework.apollo.common.http.RichResponseEntity;
import com.ctrip.framework.apollo.core.ConfigConsts;
import com.ctrip.framework.apollo.core.enums.Env;
import com.ctrip.framework.apollo.portal.component.PortalSettings;
import com.ctrip.framework.apollo.portal.entity.model.AppModel;
import com.ctrip.framework.apollo.portal.entity.po.Role;
import com.ctrip.framework.apollo.portal.entity.vo.EnvClusterInfo;
import com.ctrip.framework.apollo.portal.listener.AppCreationEvent;
import com.ctrip.framework.apollo.portal.listener.AppDeletionEvent;
import com.ctrip.framework.apollo.portal.listener.AppInfoChangedEvent;
import com.ctrip.framework.apollo.portal.service.AppService;
import com.ctrip.framework.apollo.portal.service.RoleInitializationService;
import com.ctrip.framework.apollo.portal.service.RolePermissionService;
import com.ctrip.framework.apollo.portal.spi.UserInfoHolder;
import com.ctrip.framework.apollo.portal.util.RoleUtils;
import com.google.common.collect.Sets;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.HttpClientErrorException;

import javax.validation.Valid;
import java.util.List;
import java.util.Objects;
import java.util.Set;


@RestController
@RequestMapping("/apps")
public class AppController {

  private final UserInfoHolder userInfoHolder;
  private final AppService appService;
  private final PortalSettings portalSettings;
  private final ApplicationEventPublisher publisher;
  private final RolePermissionService rolePermissionService;
  private final RoleInitializationService roleInitializationService;

  public AppController(
      final UserInfoHolder userInfoHolder,
      final AppService appService,
      final PortalSettings portalSettings,
      final ApplicationEventPublisher publisher,
      final RolePermissionService rolePermissionService,
      final RoleInitializationService roleInitializationService) {
    this.userInfoHolder = userInfoHolder;
    this.appService = appService;
    this.portalSettings = portalSettings;
    this.publisher = publisher;
    this.rolePermissionService = rolePermissionService;
    this.roleInitializationService = roleInitializationService;
  }

  @GetMapping
  public List<App> findApps(@RequestParam(value = "appIds", required = false) String appIds) {
    if (StringUtils.isEmpty(appIds)) {
      return appService.findAll();
    } else {
      return appService.findByAppIds(Sets.newHashSet(appIds.split(",")));
    }

  }

  @GetMapping("/by-owner")
  public List<App> findAppsByOwner(@RequestParam("owner") String owner, Pageable page) {
    Set<String> appIds = Sets.newHashSet();

    List<Role> userRoles = rolePermissionService.findUserRoles(owner);

    for (Role role : userRoles) {
      String appId = RoleUtils.extractAppIdFromRoleName(role.getRoleName());

      if (appId != null) {
        appIds.add(appId);
      }
    }

    return appService.findByAppIds(appIds, page);
  }

  /***
   * 创建app
   * @param appModel
   * @return
   */
  @PostMapping
  public App create(@Valid @RequestBody AppModel appModel) {
    // 将 AppModel 转换成 App 对象
    App app = transformToApp(appModel);
    // 保存 App 对象到数据库
    App createdApp = appService.createAppInLocal(app);
    // 发布 AppCreationEvent 创建app的事件，会被CreationListener监听
    publisher.publishEvent(new AppCreationEvent(createdApp));
    //获得app的管理员列表，授予App 管理员的角色
    Set<String> admins = appModel.getAdmins();
    if (!CollectionUtils.isEmpty(admins)) {//根据项目管理员创建权限
      rolePermissionService
          .assignRoleToUsers(//为管理员分配角色
                  RoleUtils.buildAppMasterRoleName(createdApp.getAppId()),//根据应用id创建角色
              admins, //管理员列表
                  userInfoHolder.getUser().getUserId()//操作员id
          );
    }

    return createdApp;
  }

  @PreAuthorize(value = "@permissionValidator.isAppAdmin(#appId)")
  @PutMapping("/{appId:.+}")
  public void update(@PathVariable String appId, @Valid @RequestBody AppModel appModel) {
    if (!Objects.equals(appId, appModel.getAppId())) {
      throw new BadRequestException("The App Id of path variable and request body is different");
    }

    App app = transformToApp(appModel);

    App updatedApp = appService.updateAppInLocal(app);

    publisher.publishEvent(new AppInfoChangedEvent(updatedApp));
  }

  @GetMapping("/{appId}/navtree")
  public MultiResponseEntity<EnvClusterInfo> nav(@PathVariable String appId) {

    MultiResponseEntity<EnvClusterInfo> response = MultiResponseEntity.ok();
    List<Env> envs = portalSettings.getActiveEnvs();
    for (Env env : envs) {
      try {
        response.addResponseEntity(RichResponseEntity.ok(appService.createEnvNavNode(env, appId)));
      } catch (Exception e) {
        response.addResponseEntity(RichResponseEntity.error(HttpStatus.INTERNAL_SERVER_ERROR,
            "load env:" + env.name() + " cluster error." + e
                .getMessage()));
      }
    }
    return response;
  }

  @PostMapping(value = "/envs/{env}", consumes = {"application/json"})
  public ResponseEntity<Void> create(@PathVariable String env, @Valid @RequestBody App app) {
    appService.createAppInRemote(Env.valueOf(env), app);

    roleInitializationService.initNamespaceSpecificEnvRoles(app.getAppId(), ConfigConsts.NAMESPACE_APPLICATION, env, userInfoHolder.getUser().getUserId());

    return ResponseEntity.ok().build();
  }

  @GetMapping("/{appId:.+}")
  public App load(@PathVariable String appId) {

    return appService.load(appId);
  }


  @PreAuthorize(value = "@permissionValidator.isSuperAdmin()")
  @DeleteMapping("/{appId:.+}")
  public void deleteApp(@PathVariable String appId) {
    App app = appService.deleteAppInLocal(appId);

    publisher.publishEvent(new AppDeletionEvent(app));
  }

  @GetMapping("/{appId}/miss_envs")
  public MultiResponseEntity<Env> findMissEnvs(@PathVariable String appId) {

    MultiResponseEntity<Env> response = MultiResponseEntity.ok();
    for (Env env : portalSettings.getActiveEnvs()) {
      try {
        appService.load(env, appId);
      } catch (Exception e) {
        if (e instanceof HttpClientErrorException &&
            ((HttpClientErrorException) e).getStatusCode() == HttpStatus.NOT_FOUND) {
          response.addResponseEntity(RichResponseEntity.ok(env));
        } else {
          response.addResponseEntity(RichResponseEntity.error(HttpStatus.INTERNAL_SERVER_ERROR,
              String.format("load appId:%s from env %s error.", appId,
                  env)
                  + e.getMessage()));
        }
      }
    }

    return response;

  }

  private App transformToApp(AppModel appModel) {
    String appId = appModel.getAppId();//获得应用id
    String appName = appModel.getName();//app 名称
    String ownerName = appModel.getOwnerName();//负责人
    String orgId = appModel.getOrgId();//部门id
    String orgName = appModel.getOrgName();//部门名称

    return App.builder()
        .appId(appId)
        .name(appName)
        .ownerName(ownerName)
        .orgId(orgId)
        .orgName(orgName)
        .build();

  }
}
