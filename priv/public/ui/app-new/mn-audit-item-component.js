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
mn.components.MnAuditItem =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnAuditItem, mn.core.MnEventableComponent);

    MnAuditItem.annotations = [
      new ng.core.Component({
        selector: "mn-audit-item",
        templateUrl: "app-new/mn-audit-item.html",
        inputs: [
          "form",
          "descriptors",
          "moduleName"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnAuditItem.prototype.ngOnInit = ngOnInit;
    MnAuditItem.prototype.mapNames = mapNames;
    MnAuditItem.prototype.generateForm = generateForm;
    MnAuditItem.prototype.doToggleAll = doToggleAll;
    MnAuditItem.prototype.setToggleAllValue = setToggleAllValue;
    MnAuditItem.prototype.maybeDisableFields = maybeDisableFields;

    return MnAuditItem;

    function MnAuditItem() {
      mn.core.MnEventableComponent.call(this);
      this.onToggleClick = new Rx.Subject();
      this.toggleSection = this.onToggleClick.pipe(Rx.operators.scan(R.not, false),
                                                   mn.core.rxOperatorsShareReplay(1));
    }

    function ngOnInit() {
      this.formHelper = new ng.forms.FormGroup({
        toggleAll: new ng.forms.FormControl()
      });
      this.thisDescriptors = this.descriptors.pipe(Rx.operators.pluck(this.moduleName));
      this.thisDescriptors
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.generateForm.bind(this));

      var thisModuleGroup = this.form.group.get("descriptors").get(this.moduleName);

      this.thisModuleChanges =
        thisModuleGroup.valueChanges.pipe(Rx.operators.startWith(thisModuleGroup.value));

      this.isAuditEnabled =
        this.form.changes.pipe(Rx.operators.pluck("auditdEnabled"),
                               Rx.operators.distinctUntilChanged());

      this.isAuditEnabled
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.maybeDisableFields.bind(this));

      this.isThereEnabledField =
        this.thisModuleChanges.pipe(Rx.operators.map(R.pipe(Object.values, R.contains(true))),
                                    mn.core.rxOperatorsShareReplay(1));

      this.thisModuleChanges
        .pipe(Rx.operators.map(R.pipe(Object.values, R.all(R.equals(true)))),
              Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.setToggleAllValue.bind(this));

      this.formHelper.get("toggleAll").valueChanges
        .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
        .subscribe(this.doToggleAll.bind(this));

    }
    function maybeDisableFields(value) {
      var method = value ? "enable" : "disable";
      this.formHelper.get("toggleAll")[method]({emitEvent: false});
      this.form.group.get("descriptors")[method]({emitEvent: false});
    }
    function setToggleAllValue(value) {
      this.formHelper.get("toggleAll").setValue(value, {emitEvent: false});
    }
    function doToggleAll(value) {
      var thisModule = this.form.group.get("descriptors").get(this.moduleName);
      var ids = Object.keys(thisModule.value);
      thisModule.patchValue(ids.reduce(function (acc, key) {
        acc[key] = value;
        return acc;
      }, {}));
    }

    function generateForm(descriptors) {
      this.form.group.get("descriptors")
        .addControl(this.moduleName, new ng.forms.FormGroup(
          descriptors.reduce(function (acc, item) {
            acc[item.id] = new ng.forms.FormControl(item.value);
            return acc;
          }.bind(this), {})
        ));
    }

    function mapNames(name) {
      switch (name) {
      case "auditd":
        return "Audit";
      case "ns_server":
        return "Server";
      case "n1ql":
        return "Query and Index Service";
      case "eventing":
        return "Eventing Service";
      case "memcached":
        return "Data Service";
      case "xdcr":
        return name.toUpperCase();
      case "fts":
        return "Search Service";
      default:
        return name.charAt(0).toUpperCase() + name.substr(1).toLowerCase();
      }
    }

  })(window.rxjs);
