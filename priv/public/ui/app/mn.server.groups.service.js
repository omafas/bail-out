/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {BehaviorSubject, combineLatest, timer} from 'rxjs';
import {map, shareReplay, switchMap, pluck,
  distinctUntilChanged} from 'rxjs/operators';
import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';

import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnAdminService} from './mn.admin.service.js';
import {singletonGuard} from './mn.core.js';

export {MnServerGroupsService}

class MnServerGroupsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnAdminService,
    MnPermissions
  ]}

  constructor(http, mnAdminService, mnPermissions) {
    singletonGuard(MnServerGroupsService);
    this.http = http;
    let permissionsStream = mnPermissions.stream;

    this.stream = {};

    let getServerGroups =
      (new BehaviorSubject()).pipe(
        switchMap(this.getServerGroups.bind(this)));

    let nodesWithGroupName =
        combineLatest(getServerGroups,
                      mnAdminService.stream.getNodes)
        .pipe(map(this.addGroupNameToNodes.bind(this)));

    let serverGroupsReadStream =
        permissionsStream.pipe(pluck('cluster','server_groups','read'),
                               distinctUntilChanged());

    this.stream.maybeGetServersWithGroups =
      combineLatest(mnAdminService.stream.isGroupsAvailable,
                    serverGroupsReadStream,
                    timer(0, 10000))
      .pipe(switchMap(([isGroupsAvailable, serverGroupsRead,]) => {
        let hasGroups = isGroupsAvailable && serverGroupsRead;
        return hasGroups ? nodesWithGroupName : mnAdminService.stream.getNodes;
      }),
            shareReplay({refCount: true, bufferSize: 1}));
  }

  getServerGroups() {
    return this.http.get("/pools/default/serverGroups");
  }

  addGroupNameToNodes([groups, nodes]) {
    let nodesMap = {};

    groups.groups.forEach(group =>
      group.nodes.forEach(node =>
        nodesMap[node.otpNode] = group.name));

    nodes.forEach(node =>
      node.groupName = nodesMap[node.otpNode]);

    return nodes;
  }
}
