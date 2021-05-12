/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.components = mn.components || {};
mn.components.MnAudit =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAudit, mn.core.MnEventableComponent);

    MnAudit.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-audit.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnAudit.parameters = [
      mn.services.MnForm,
      mn.services.MnHelper,
      mn.services.MnSecurity,
      mn.services.MnPermissions,
      mn.services.MnAlerts,
      mn.services.MnAdmin,
      mn.services.MnPools
    ];

    MnAudit.prototype.unpackGetAudit = unpackGetAudit;
    MnAudit.prototype.maybeDisableFields = maybeDisableFields;
    MnAudit.prototype.disableEnableFiled = disableEnableFiled;
    MnAudit.prototype.getDescriptorsByModule = getDescriptorsByModule;
    MnAudit.prototype.getDisabledByID = getDisabledByID;
    MnAudit.prototype.getEnding = getEnding;
    MnAudit.prototype.prepareDataForSending = prepareDataForSending;

    return MnAudit;

    function MnAudit(mnFormService, mnHelperService, mnSecurityService, mnPermissionsService, mnAlertsService, mnAdminService, mnPoolsService) {
      mn.core.MnEventableComponent.call(this);

      this.IEC = mnHelperService.IEC;
      this.securityWrite = mnPermissionsService.createPermissionStream("admin.security!write");
      this.compatVersion55 = mnAdminService.stream.compatVersion55;
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.getAuditDescriptors = mnSecurityService.stream.getAuditDescriptors;
      this.getAudit = mnSecurityService.stream.getAudit;
      this.postAudit = mnSecurityService.stream.postAudit;
      this.postAuditValidation = mnSecurityService.stream.postAuditValidation;

      this.form = mnFormService.create(this);
      this.form
        .setFormGroup({auditdEnabled: null,
                       logPath: null,
                       rotateInterval: null,
                       rotateSize: null,
                       rotateUnit: null,
                       descriptors: this.form.builder.group({}),
                       disabledUsers: null})
        .setUnpackPipe(Rx.pipe(Rx.operators.map(this.unpackGetAudit.bind(this))))
        .setPackPipe(Rx.pipe(Rx.operators.withLatestFrom(this.compatVersion55,this.isEnterprise),
                             Rx.operators.map(this.prepareDataForSending.bind(this))))
        .setSource(this.getAudit)
        .setPostRequest(this.postAudit)
        .setValidation(this.postAuditValidation, this.securityWrite)
        .clearErrors()
        .successMessage("Settings saved successfully!");

      this.httpError = Rx.merge(this.postAudit.error, this.postAuditValidation.error);

      this.maybeItIsPlural =
        this.form.changes.pipe(Rx.operators.pluck("rotateInterval"),
                               Rx.operators.distinctUntilChanged(),
                               Rx.operators.map(this.getEnding.bind(this)),
                               mn.core.rxOperatorsShareReplay(1));

      Rx.combineLatest(
        this.form.changes.pipe(Rx.operators.pluck("auditdEnabled"),
                               Rx.operators.distinctUntilChanged()),
        this.securityWrite)
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableFields.bind(this));

      this.securityWrite
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.disableEnableFiled.bind(this));

      var disabledByID =
          this.getAudit.pipe(Rx.operators.pluck("disabled"),
                             Rx.operators.map(this.getDisabledByID.bind(this)));

      this.descriptorsByModule =
        Rx.combineLatest(this.getAuditDescriptors, disabledByID)
        .pipe(Rx.operators.map(this.getDescriptorsByModule.bind(this)),
              mn.core.rxOperatorsShareReplay(1));
    }

    function formatTimeUnit(unit) {
      switch (unit) {
      case 'minutes': return 60;
      case 'hours': return 3600;
      case 'days': return 86400;
      }
    }

    function prepareDataForSending(parameters) {
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
        if (value.disabledUsers) {
          result.disabledUsers = value.disabledUsers.replace(/\/couchbase/gi,"/local");
        }
      }
      if (value.auditdEnabled) {
        result.rotateInterval = value.rotateInterval * formatTimeUnit(value.rotateUnit);
        result.logPath = value.logPath;
        result.rotateSize = value.rotateSize;
      }
      if (value.rotateSize) {
        result.rotateSize = value.rotateSize * this.IEC.Mi;
      }
      return result;
    }

    function getDisabledByID(disabled) {
      return disabled.reduce(function (acc, item) {
        acc[item] = true;
        return acc;
      }, {});
    }

    function getEnding(value) {
      return value !== 1 ? "s" : "";
    }

    function getDescriptorsByModule(data) {
      return data[0].reduce(function (acc, item) {
        acc[item.module] = acc[item.module] || [];
        item.value = !data[1][item.id];
        acc[item.module].push(item);
        return acc;
      }, {});
    }

    function disableEnableFiled(value) {
      var method = value ? "enable" : "disable";
      this.form.group.get("auditdEnabled")[method]({emitEvent: false});
    }

    function maybeDisableFields(values) {
      var settings = {emitEvent: false};
      var method = (values[1] && values[0]) ? "enable" : "disable";
      this.form.group.get("logPath")[method](settings);
      this.form.group.get("rotateInterval")[method](settings);
      this.form.group.get("rotateSize")[method](settings);
      this.form.group.get("rotateUnit")[method](settings);
      this.form.group.get("disabledUsers")[method](settings);
    }

    function unpackGetAudit(data) {
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

  })(window.rxjs);
