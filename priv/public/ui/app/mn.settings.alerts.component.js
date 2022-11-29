/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {UIRouter} from '@uirouter/angular';
import {takeUntil, map, pluck, withLatestFrom } from 'rxjs/operators';
import {pipe} from 'rxjs';
import {FormControl, FormGroup, Validators} from '@angular/forms';

import { MnLifeCycleHooksToStream } from './mn.core.js';
import { MnFormService } from './mn.form.service.js';
import { MnSettingsAlertsService } from './mn.settings.alerts.service.js';
import { MnPermissions } from './ajs.upgraded.providers.js';
import { knownAlerts } from './constants/constants.js';
import template from "./mn.settings.alerts.html";

export { MnSettingsAlertsComponent };

class MnSettingsAlertsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnSettingsAlertsService,
    MnPermissions,
    MnFormService,
    UIRouter
  ]}

  constructor(mnSettingsAlertsService, mnPermissions, mnFormService, uiRouter) {
    super();

    this.getAlerts = mnSettingsAlertsService.stream.getAlerts;
    this.saveAlerts = mnSettingsAlertsService.stream.saveAlerts;
    this.testMail = mnSettingsAlertsService.stream.testMail;
    this.knownAlerts = knownAlerts;
    this.getAlertLabel = this.getAlertLabel.bind(this);
    this.cancel = this.cancel.bind(this);
    this.permissions = mnPermissions.stream;
    this.uiRouter = uiRouter;

    this.form = mnFormService.create(this)
      .setFormGroup(this.getInitialValues())
      .setUnpackPipe(pipe(map(this.unpackGetAlerts.bind(this))))
      .setPackPipe(map(this.getAlertsData.bind(this)))
      .setSource(this.getAlerts)
      .setPostRequest(this.saveAlerts)
      .successMessage("Settings saved successfully!")
      .clearErrors()
      .showGlobalSpinner();

    this.testEmailForm = mnFormService.create(this)
      .setFormGroup({})
      .setPackPipe(map(this.packTestEmailData.bind(this)))
      .setPostRequest(this.testMail)
      .successMessage("Test email was sent successfully!")
      .errorMessage("An error occurred sending the test email.")
      .showGlobalSpinner();

    this.form.group.disable();

    this.isDisabled = this.getAlerts
          .pipe(withLatestFrom(this.permissions),
                map(([, permissions]) => permissions.cluster.settings.write));

    this.isDisabled
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(this.toggleInitialEnabledFields.bind(this));

    this.form.group.get('enabled').valueChanges
      .pipe(withLatestFrom(this.permissions),
            takeUntil(this.mnOnDestroy))
      .subscribe(this.toggleEnabled.bind(this))

    this.testEmailIsDisabled =
      this.form.group.get('enabled').valueChanges
          .pipe(withLatestFrom(this.permissions
                                   .pipe(pluck('cluster', 'settings', 'write'))),
                map(([enabled, hasPermissions]) => enabled && hasPermissions));
  }

  getInitialValues() {
    return {
      config: this.addConfigGroup(),
      alerts: this.addAlertGroup(),
      popUpAlerts: this.addAlertGroup(),
      enabled: false,
    };
  }

  toggleInitialEnabledFields(value) {
    this.form.group.get('enabled')[value ? "enable" : "disable"]();
    this.form.group.get('popUpAlerts')[value ? "enable" : "disable"]();
  }

  addConfigGroup() {
    return new FormGroup({
      emailEncrypt: new FormControl(false),
      emailHost: new FormControl(""),
      emailPort: new FormControl("", [Validators.min(0)]),
      emailUser: new FormControl(""),
      emailPass: new FormControl(""),
      recipients: new FormControl(""),
      sender: new FormControl("")
    });
  }

  addAlertGroup() {
    return new FormGroup(
      knownAlerts.reduce(function (acc, item) {
        acc[item] = new FormControl({
          value: "",
          disabled: true
        });
        return acc;
      }, {}));
  }

  toggleEnabled([enabled, permissions]) {
    if (permissions.cluster.settings.write) {
      this.form.group.get('alerts')[enabled ? 'enable' : 'disable']();
      this.form.group.get('config')[enabled ? 'enable' : 'disable']();
    }
  }

  unpackGetAlerts(data) {
    let arrayToObject = arr =>
        arr.reduce((o, key) => ({ ...o, [key]: true }), {});

    return {
      config: {
        emailEncrypt: data.emailServer.encrypt,
        emailHost: data.emailServer.host,
        emailPort: data.emailServer.port,
        emailUser: data.emailServer.user,
        emailPass: data.emailServer.pass,
        recipients: data.recipients.join(","),
        sender: data.sender
      },
      enabled: data.enabled,
      alerts: arrayToObject(data.alerts),
      popUpAlerts: arrayToObject(data.pop_up_alerts)
    };
  }

  getAlertsData() {
    let stringifyValues = obj =>
        Object.keys(obj).filter(v => obj[v]).join(',');

    let packedData = {
      alerts: stringifyValues(this.form.group.get('alerts').value),
      pop_up_alerts: stringifyValues(this.form.group.get('popUpAlerts').value),
      enabled: this.form.group.get('enabled').value
    };

    return Object.assign(packedData, this.form.group.get('config').getRawValue());
  }

  packTestEmailData() {
    let params = this.getAlertsData();
    params.subject = 'Test email from Couchbase Server';
    params.body = 'This email was sent to you to test the email alert email server settings.';
    return params;
  }

  cancel() {
    this.uiRouter.stateService.reload('app.admin.settings.alerts');
  }

  getAlertLabel(alert) {
    switch (alert) {
      case knownAlerts[0]: return 'Node was auto-failed-over';
      case knownAlerts[1]: return 'Maximum number of auto-failed-over nodes was reached';
      case knownAlerts[2]: return 'Node was not auto-failed-over as other nodes are down at the same time';
      case knownAlerts[3]: return 'Node was not auto-failed-over as there are not enough nodes in the cluster running the same service';
      case knownAlerts[4]: return 'Node was not auto-failed-over as auto-failover for one or more services running on the node is disabled';
      case knownAlerts[5]: return 'Node\'s IP address has changed unexpectedly';
      case knownAlerts[6]: return 'Disk space used for persistent storage has reached at least 90% of capacity';
      case knownAlerts[7]: return 'Metadata overhead is more than 50%';
      case knownAlerts[8]: return 'Bucket memory on a node is entirely used for metadata';
      case knownAlerts[9]: return 'Writing data to disk for a specific bucket has failed';
      case knownAlerts[10]: return 'Writing event to audit log has failed';
      case knownAlerts[11]: return 'Approaching full Indexer RAM warning';
      case knownAlerts[12]: return 'Remote mutation timestamp exceeded drift threshold';
      case knownAlerts[13]: return 'Communication issues among some nodes in the cluster';
      case knownAlerts[14]: return 'Node\'s time is out of sync with some nodes in the cluster';
      case knownAlerts[15]: return 'Disk usage analyzer is stuck; cannot fetch disk usage data';
      case knownAlerts[16]: return 'Memory usage threshold exceeded';
      case knownAlerts[17]: return 'History size threshold exceeded';
    }
  }
}
