/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '/ui/web_modules/@angular/core.js';
import {MnPoolsService} from './mn.pools.service.js';
import {MnWizardService} from './mn.wizard.service.js';
import {BehaviorSubject} from '/ui/web_modules/rxjs.js';
import {takeUntil, pluck, distinctUntilChanged} from '/ui/web_modules/rxjs/operators.js';
import {MnLifeCycleHooksToStream} from './mn.core.js';

export {MnHostnameConfigComponent};

class MnHostnameConfigComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-hostname-config",
      templateUrl: "/ui/app/mn.hostname.config.html",
      inputs: [
        "group",
        "isHostCfgEnabled"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnWizardService,
    MnPoolsService
  ]}

  constructor(mnWizardService, mnPoolsService) {
    super();
    this.focusFieldSubject = new BehaviorSubject(true);
    this.hostnameHttp = mnWizardService.stream.hostnameHttp;
    this.setupNetConfigHttp = mnWizardService.stream.setupNetConfigHttp;
    this.enableExternalListenerHttp = mnWizardService.stream.enableExternalListenerHttp;
    this.isEnterprise = mnPoolsService.stream.isEnterprise;
  }

  ngOnInit() {
    if (!this.isHostCfgEnabled) {
      return;
    }
    this.group.valueChanges
      .pipe(pluck("hostConfig", "afamily"),
            distinctUntilChanged(),
            takeUntil(this.mnOnDestroy))
      .subscribe((afamily) => {
        var hostname = this.group.get("hostname").value;
        if (afamily && hostname == "127.0.0.1") {
          this.group.get("hostname").setValue("::1");
        }
        if (!afamily && hostname == "::1") {
          this.group.get("hostname").setValue("127.0.0.1");
        }
      });
  }
}
