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
import {merge, combineLatest, pipe, of} from 'rxjs';

import {MnLifeCycleHooksToStream} from './mn.core.js';

import {MnFormService} from './mn.form.service.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnSecurityService} from './mn.security.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnAdminService} from './mn.admin.service.js';
import {MnPoolsService} from './mn.pools.service.js';

export {MnSecurityAuditComponent};

class MnSecurityAuditComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      templateUrl: "app/mn.security.audit.html",
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
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
    this.getAuditDescriptors = mnSecurityService.stream.getAuditDescriptors;
    this.getAudit = mnSecurityService.stream.getAudit;
    this.postAudit = mnSecurityService.stream.postAudit;
    this.postAuditValidation = mnSecurityService.stream.postAuditValidation;

    this.securityWrite = mnPermissions.stream
      .pipe(map(permissions => permissions.cluster.admin.security.write));

    this.form = mnFormService.create(this);
    this.form
      .setFormGroup({auditdEnabled: null,
                     logPath: null,
                     rotateInterval: null,
                     rotateSize: null,
                     rotateUnit: null,
                     descriptors: this.form.builder.group({}),
                     disabledUsers: null})
      .setUnpackPipe(pipe(map(this.unpackGetAudit.bind(this))))
      .setPackPipe(pipe(withLatestFrom(this.compatVersion55, this.isEnterprise),
                        map(this.prepareDataForSending.bind(this))))
      .setSource(this.getAudit)
      .setPostRequest(this.postAudit)
      .setValidation(this.postAuditValidation, this.securityWrite)
      .clearErrors()
      .showGlobalSpinner()
      .successMessage("Settings saved successfully!");

    this.httpError = merge(this.postAudit.error, this.postAuditValidation.error);

    this.maybeItIsPlural =
      this.form.group.get("rotateInterval").valueChanges.pipe(
                                        distinctUntilChanged(),
                                        map(this.getEnding.bind(this)),
                                        shareReplay({refCount: true, bufferSize: 1}));

    combineLatest(
      this.form.group.valueChanges.pipe(pluck("auditdEnabled"),
                                        distinctUntilChanged()),
      this.securityWrite)
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.maybeDisableFields.bind(this));

    this.securityWrite
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.disableEnableFiled.bind(this));

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
  }

  formatTimeUnit(unit) {
    switch (unit) {
    case 'minutes': return 60;
    case 'hours': return 3600;
    case 'days': return 86400;
    }
  }

  prepareDataForSending(parameters) {
    var value = this.form.group.value;
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

  disableEnableFiled(value) {
    var method = value ? "enable" : "disable";
    this.form.group.get("auditdEnabled")[method]({emitEvent: false});
  }

  maybeDisableFields(values) {
    var settings = {emitEvent: false};
    var method = (values[1] && values[0]) ? "enable" : "disable";
    this.form.group.get("logPath")[method](settings);
    this.form.group.get("rotateInterval")[method](settings);
    this.form.group.get("rotateSize")[method](settings);
    this.form.group.get("rotateUnit")[method](settings);
    this.form.group.get("disabledUsers")[method](settings);
  }

  unpackGetAudit(data) {
    if (data.rotateInterval % 86400 == 0) {
      data.rotateInterval /= 86400;
      data.rotateUnit = 'days';
    } else if (data.rotateInterval % 3600 == 0) {
      data.rotateInterval /= 3600;
      data.rotateUnit = 'hours';
    } else {
      data.rotateInterval /= 60;
      data.rotateUnit = 'minutes';
    }
    if (data.rotateSize) {
      data.rotateSize = data.rotateSize / this.IEC.Mi;
    }
    if (data.disabledUsers) {
      data.disabledUsers = data.disabledUsers.map(function (user) {
        return user.name + "/" + (user.domain === "local" ? "couchbase" : user.domain);
      }).join(',');
    }
    return data;
  }
}
