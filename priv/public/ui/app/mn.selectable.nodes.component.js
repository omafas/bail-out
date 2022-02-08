/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {takeUntil, withLatestFrom} from 'rxjs/operators';

import {MnLifeCycleHooksToStream} from './mn.core.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnServerGroupsService} from './mn.server.groups.service.js';
import {MnFormatServices} from './mn.pipes.js';

export {MnSelectableNodesComponent};

class MnSelectableNodesComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-selectable-nodes",
      templateUrl: "/ui/app/mn.selectable.nodes.html",
      inputs: [
        "mnSelectAll",
        "mnGroup"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnHelperService,
    MnServerGroupsService,
    MnFormatServices
  ]}

  constructor(mnHelperService, mnServerGroupsService, mnFormatServices) {
    super();

    this.mnHelperService = mnHelperService;
    this.mnFormatServices = mnFormatServices;

    this.filter = mnHelperService.createFilter(this, ['hostname', 'groupName', 'services', 'status'], true, this.prepareFilteredValue.bind(this));

    let nodesWithGroupName = mnServerGroupsService.stream.maybeGetServersWithGroups;
    this.nodes = nodesWithGroupName.pipe(this.filter.pipe);
  }

  ngOnInit() {
    if (this.mnSelectAll) {
      this.toggler = this.mnHelperService.createToggle();
      this.toggler.state
        .pipe(withLatestFrom(this.nodes),
              takeUntil(this.mnOnDestroy))
        .subscribe(this.toggleAllNodes.bind(this));
    }
  }

  toggleAllNodes([isChecked, filteredNodes]) {
    let nodeValues = this.mnGroup.value;
    filteredNodes.forEach(node => {
      if (!this.mnGroup.controls[node.otpNode].disabled) {
        nodeValues[node.otpNode] = isChecked;
      }
    });
    this.mnGroup.patchValue(nodeValues);
  }

  trackByMethod(index, node) {
    return node.otpNode;
  }

  prepareFilteredValue(key, value) {
    if (key === 'services') {
      return value.map(this.mnFormatServices.transform).join(' ');
    }

    return value;
  }
}
