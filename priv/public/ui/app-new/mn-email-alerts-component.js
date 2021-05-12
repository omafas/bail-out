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
mn.components.MnEmailAlerts =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnEmailAlerts, mn.core.MnEventableComponent);

    MnEmailAlerts.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-email-alerts.html",
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnEmailAlerts.parameters = [
      mn.services.MnForm,
      mn.services.MnSettings,
      mn.services.MnPermissions,
      mn.services.MnAlerts,
      mn.services.MnAdmin
    ];

    MnEmailAlerts.prototype.maybeDisableFields = maybeDisableFields;
    MnEmailAlerts.prototype.disableEnableFiled = disableEnableFiled
    MnEmailAlerts.prototype.unpack = unpack;
    MnEmailAlerts.prototype.getAlertDescription = getAlertDescription;
    MnEmailAlerts.prototype.prepareDataForSending = prepareDataForSending;

    var alerts = ["auto_failover_node",
                  "auto_failover_maximum_reached",
                  "auto_failover_other_nodes_down",
                  "auto_failover_cluster_too_small",
                  "auto_failover_disabled",
                  "ip",
                  "disk",
                  "overhead",
                  "ep_oom_errors",
                  "ep_item_commit_failed",
                  "audit_dropped_events",
                  "indexer_ram_max_usage",
                  "ep_clock_cas_drift_threshold_exceeded",
                  "communication_issue",
                  "time_out_of_sync"
                 ];

    return MnEmailAlerts;

    function MnEmailAlerts(mnFormService, mnSettingsService, mnPermissionsService, mnAlertsService, mnAdminService) {
      mn.core.MnEventableComponent.call(this);

      this.alerts = alerts;
      this.postAlerts = mnSettingsService.stream.postAlerts;
      this.postAlertsValidation = mnSettingsService.stream.postAlertsValidation;
      this.postTestEmail = mnSettingsService.stream.postTestEmail;
      this.settingsWrite = mnPermissionsService.createPermissionStream("admin.settings!write");

      this.form = mnFormService.create(this);
      this.form
        .setFormGroup({
          enabled: null,
          emailServer: this.form.builder.group({
            user: null,
            pass: null,
            host: null,
            port: null,
            encrypt: null
          }),
          recipients: null,
          sender: null,
          alerts: this.form.builder.group(alerts.reduce(function (acc, key) {
            return R.assoc(key, null, acc);
          }, {}))
        })
        .setUnpackPipe(Rx.operators.map(this.unpack.bind(this)))
        .setPackPipe(Rx.operators.map(this.prepareDataForSending.bind(this)))
        .setSource(mnSettingsService.stream.getAlerts)
        .setPostRequest(this.postAlerts)
        .setValidation(this.postAlertsValidation, this.settingsWrite)
        .clearErrors()
        .successMessage("Settings saved successfully!");

      this.onTestEmail = new Rx.Subject();

      this.httpError = Rx.merge(this.postAlerts.error, this.postAlertsValidation.error);

      Rx.combineLatest(
        this.form.changes.pipe(Rx.operators.pluck("enabled"),
                               Rx.operators.distinctUntilChanged()),
        this.settingsWrite)
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableFields.bind(this));

      this.settingsWrite
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.disableEnableFiled.bind(this));
    }

    function prepareDataForSending(data) {
      var result = {};
      result.enabled = this.form.group.get("enabled").value;
      result.sender = this.form.group.get("sender").value;
      result.recipients = this.form.group.get("recipients").value.replace(/\s+/g, ',');
      result.emailUser = this.form.group.get("emailServer.user").value;
      result.emailPass = this.form.group.get("emailServer.pass").value;
      result.emailHost = this.form.group.get("emailServer.host").value;
      result.emailPort = this.form.group.get("emailServer.port").value;
      result.emailEncrypt = this.form.group.get("emailServer.encrypt").value;
      result.alerts =
        Object.keys(R.pickBy(R.equals(true), this.form.group.get("alerts").value)).join(',');
      return result;
    }

    function unpack(v) {
      var copy = Object.assign({}, v);
      copy.recipients = v.recipients.join('\n');
      copy.alerts = v.alerts.reduce(function (acc, key) {
        return R.assoc(key, true, acc);
      }, {})
      return copy;
    }

    function getAlertDescription(name) {
      switch (name) {
      case this.alerts[0]: return 'Node was auto-failed-over';
      case this.alerts[1]: return 'Maximum number of auto-failed-over nodes was reached';
      case this.alerts[2]: return 'Node wasn\'t auto-failed-over as other nodes are down at the same time';
      case this.alerts[3]: return 'Node was not auto-failed-over as there are not enough nodes in the cluster running the same service';
      case this.alerts[4]: return 'Node was not auto-failed-over as auto-failover for one or more services running on the node is disabled';
      case this.alerts[5]: return 'Node\'s IP address has changed unexpectedly';
      case this.alerts[6]: return 'Disk space used for persistent storage has reached at least 90% of capacity';
      case this.alerts[7]: return 'Metadata overhead is more than 50%';
      case this.alerts[8]: return 'Bucket memory on a node is entirely used for metadata';
      case this.alerts[9]: return 'Writing data to disk for a specific bucket has failed';
      case this.alerts[10]: return 'Writing event to audit log has failed';
      case this.alerts[11]: return 'Approaching full Indexer RAM warning';
      case this.alerts[12]: return 'Remote mutation timestamp exceeded drift threshold';
      case this.alerts[13]: return 'Communication issues among some nodes in the cluster';
      }
    }

    function disableEnableFiled(value) {
      var method = value ? "enable" : "disable";
      this.form.group.get("enabled")[method]({emitEvent: false});
    }
    function maybeDisableFields(values) {
      var settings = {emitEvent: false};
      var method = (values[1] && values[0]) ? "enable" : "disable";
      this.form.group.get("emailServer")[method](settings);
      this.form.group.get("recipients")[method](settings);
      this.form.group.get("sender")[method](settings);
      this.form.group.get("alerts")[method](settings);
    }

  })(window.rxjs);
