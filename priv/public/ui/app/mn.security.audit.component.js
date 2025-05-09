/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {map, withLatestFrom, pluck, switchMap,
        distinctUntilChanged, shareReplay, takeUntil} from 'rxjs/operators';
import {merge, combineLatest, pipe, of, NEVER} from 'rxjs';

import {MnLifeCycleHooksToStream} from './mn.core.js';

import {MnFormService} from './mn.form.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnHttpGroupRequest} from './mn.http.request.js';
import template from "./mn.security.audit.html";

export {MnSecurityAuditComponent};

class MnSecurityAuditComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnFormService,
    MnHelperService,
    MnSecurityService,
    MnPermissions,
    MnAdminService,
    MnPoolsService
  ]}

  constructor(mnFormService, mnHelperService, mnSecurityService, mnPermissions, mnAdminService, mnPoolsService) {
    super();

    this.IEC = mnHelperService.IEC;

    this.compatVersion55 = mnAdminService.stream.compatVersion55;
    this.compatVersion80 = mnAdminService.stream.compatVersion80;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.getAuditDescriptors = mnSecurityService.stream.getAuditDescriptors;
    this.getAudit = mnSecurityService.stream.getAudit;
    this.getUIUserRoles = mnSecurityService.stream.getUIUserRoles;
    this.getUIUserGroups = mnSecurityService.stream.getUIUserGroups;
    this.postAudit = mnSecurityService.stream.postAudit;
    this.postAuditValidation = mnSecurityService.stream.postAuditValidation;
    this.getUserActivity = mnSecurityService.stream.getUserActivity;
    this.postUserActivity = mnSecurityService.stream.postUserActivity;
    this.postUserActivityValidation = mnSecurityService.stream.postUserActivityValidation;

    this.securityWrite = mnPermissions.stream
      .pipe(map(permissions => permissions.cluster.admin.security.write));

    this.getAuditInfo = combineLatest(this.compatVersion80, this.isEnterprise)
      .pipe(switchMap(([compat80, isEnterprise]) =>
             (compat80 && isEnterprise) ? combineLatest(this.getAudit, this.getUserActivity, this.getUIUserRoles, this.getUIUserGroups) : this.getAudit),
            shareReplay({refCount: true, bufferSize: 1}));

    this.form = mnFormService.create(this);

    this.form
      .setFormGroup({
        auditEvents: this.form.builder.group({
          auditdEnabled: null,
          logPath: null,
          rotateInterval: null,
          rotateSize: null,
          rotateUnit: null,
          descriptors: this.form.builder.group({}),
          disabledUsers: null
        }),
        userActivity: this.form.builder.group({
          enabled: false,
          roleDescriptors: this.form.builder.group({}),
          groupDescriptors: this.form.builder.group({})
        })
      })
      .setUnpackPipe(pipe(map(this.unpackInfo.bind(this))))
      .setPackPipe(pipe(withLatestFrom(this.compatVersion55, this.isEnterprise),
                        map(this.prepareAuditDataForSending.bind(this))))
      .setSource(this.getAuditInfo)
      .setPostRequest(this.postAudit)
      .setValidation(this.postAuditValidation, this.securityWrite)
      .setPackPipe(pipe(withLatestFrom(this.compatVersion55, this.isEnterprise),
        map(this.prepareUserActivityDataForSending.bind(this))))
      .setPostRequest(this.postUserActivity)
      .setValidation(this.postUserActivityValidation, this.securityWrite)
      .clearErrors()
      .showGlobalSpinner()
      .successMessage("Settings saved successfully!");

    this.httpErrorAudit = merge(
      this.postAudit.error,
      this.postAuditValidation.error);

    this.httpErrorUserActivity = merge(
      this.postUserActivity.error,
      this.postUserActivityValidation.error);


    this.maybeItIsPlural =
      this.form.group.get('auditEvents.rotateInterval').valueChanges.pipe(
                                        distinctUntilChanged(),
                                        map(this.getEnding.bind(this)),
                                        shareReplay({refCount: true, bufferSize: 1}));

    combineLatest(
      this.form.group.get('auditEvents').valueChanges.pipe(pluck("auditdEnabled"),
                                        distinctUntilChanged()),
      this.securityWrite)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableAuditFields.bind(this));

    this.securityWrite
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableToggles.bind(this));

    var disabledByID =
        this.getAudit.pipe(pluck("disabled"),
                           map(this.getDisabledByID.bind(this)));

    this.descriptorsByModule =
      combineLatest(this.getAuditDescriptors, disabledByID,
                    mnAdminService.stream.compatVersion65
                    .pipe(switchMap(is65 => is65 ?
                                    mnSecurityService.stream.getAuditNonFilterableDescriptors :
                                    of(null))))
      .pipe(map(this.getDescriptorsByModule.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));


    this.userActivityUIRoles = combineLatest(this.getUIUserRoles, this.getUserActivity)
      .pipe(map(this.getUIUserRolesMap.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.userActivityUIGroups = combineLatest(this.getUIUserGroups, this.getUserActivity)
    .pipe(map(this.getUIUserGroupsMap.bind(this)),
      shareReplay({refCount: true, bufferSize: 1}));

    this.userActivitySelectedTab = 'roles';
  }

  formatTimeUnit(unit) {
    switch (unit) {
    case 'minutes': return 60;
    case 'hours': return 3600;
    case 'days': return 86400;
    }
  }

  prepareAuditDataForSending(parameters) {
    var value = this.form.group.value.auditEvents;
    var result = {auditdEnabled: value.auditdEnabled};
    var compatVersion55 = parameters[1];
    var isEnterprise = parameters[2];

    if (compatVersion55 && isEnterprise) {
      if (value.descriptors) {
        result.disabled = [];
        Object.keys(value.descriptors).forEach(function(key) {
          Object.keys(value.descriptors[key]).forEach(function (id) {
            !value.descriptors[key][id] && result.disabled.push(id);
          });
        });
        result.disabled = result.disabled.join(',');
      }
      var users = value.disabledUsers;
      result.disabledUsers = users ? users.replace(/\/couchbase/gi,"/local") : "";
    }
    if (value.auditdEnabled) {
      result.rotateInterval = value.rotateInterval * this.formatTimeUnit(value.rotateUnit);
      result.logPath = value.logPath;
      result.rotateSize = value.rotateSize;
    }
    if (value.rotateSize) {
      result.rotateSize = value.rotateSize * this.IEC.Mi;
    }
    return result;
  }

  prepareUserActivityDataForSending(parameters) {
    let value = this.form.group.value.userActivity;
    let result = {enabled: value.enabled, trackedRoles: [], trackedGroups: []};
    if (value.roleDescriptors) {
      Object.values(value.roleDescriptors).forEach((descriptor) => {
        Object.entries(descriptor).forEach(([role, value]) => {
          if (value) {
            result.trackedRoles.push(role);
          }
        })
      });
    }
    if (value.groupDescriptors) {
      Object.keys(value.groupDescriptors).forEach((group) => {
        if (value.groupDescriptors[group]) {
          result.trackedGroups.push(group);
        }
      });
    }
    return result;
  }

  getDisabledByID(disabled) {
    return disabled.reduce(function (acc, item) {
      acc[item] = true;
      return acc;
    }, {});
  }

  getEnding(value) {
    return value != 1 ? "s" : "";
  }

  getDescriptorsByModule(data) {
    if (data[2]) {
      Array.prototype.push.apply(data[0], data[2]);
    }
    return data[0].reduce(function (acc, item) {
      acc[item.module] = acc[item.module] || [];
      item.value = !data[1][item.id];
      acc[item.module].push(item);
      return acc;
    }, {});
  }

  maybeDisableToggles(value) {
    var method = value ? "enable" : "disable";
    this.form.group.get('auditEvents.auditdEnabled')[method]({emitEvent: false});
    this.form.group.get('userActivity.enabled')[method]({emitEvent: false});
  }

  maybeDisableAuditFields(values) {
    var settings = {emitEvent: false};
    var method = (values[1] && values[0]) ? "enable" : "disable";
    this.form.group.get('auditEvents.logPath')[method](settings);
    this.form.group.get('auditEvents.rotateInterval')[method](settings);
    this.form.group.get('auditEvents.rotateSize')[method](settings);
    this.form.group.get('auditEvents.rotateUnit')[method](settings);
    this.form.group.get('auditEvents.disabledUsers')[method](settings);
  }

  unpackInfo(info) {
    var auditData;
    var userActivityData;
    var hasUserActivity = info instanceof Array
    if (hasUserActivity) {
      auditData = info[0];
      userActivityData = info[1];
    } else {
      auditData = info;
    }

    if (auditData.rotateInterval % 86400 == 0) {
      auditData.rotateInterval /= 86400;
      auditData.rotateUnit = 'days';
    } else if (auditData.rotateInterval % 3600 == 0) {
      auditData.rotateInterval /= 3600;
      auditData.rotateUnit = 'hours';
    } else {
      auditData.rotateInterval /= 60;
      auditData.rotateUnit = 'minutes';
    }
    if (auditData.rotateSize) {
      auditData.rotateSize = auditData.rotateSize / this.IEC.Mi;
    }
    if (auditData.disabledUsers) {
      auditData.disabledUsers = auditData.disabledUsers.map(function (user) {
        return user.name + "/" + (user.domain === "local" ? "couchbase" : user.domain);
      }).join(',');
    }

    let result = {auditEvents: auditData};
    if (hasUserActivity) {
      result.userActivity = {enabled: userActivityData.enabled};
    }

    return result;
  }

  getUIUserRolesMap([uiRoles, userActivity]) {
    return uiRoles.folders.reduce((acc, item) => {
      item.roles.forEach(role => {
        role.value = userActivity.trackedRoles.includes(role.role);
      })
       acc[item.name] = item.roles;
       return acc;
     }, {});
  }

  getUIUserGroupsMap([uiGroups, userActivity]) {
    return uiGroups.reduce((acc, item) => {
      acc[item.id] = {
        description: item.description,
        value: userActivity.trackedGroups.includes(item.id)
      };
      return acc;
    }, {});
  }
}
