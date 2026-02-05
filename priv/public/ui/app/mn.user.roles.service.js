/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';

import {singletonGuard} from './mn.core.js';

export {MnUserRolesService};

class MnUserRolesService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient
  ]}

  constructor(http) {
    singletonGuard(MnUserRolesService);
    this.http = http;
  }

  getUsers(params) {
    return this.http.get('/settings/rbac/users', {
      params: new HttpParams()
        .set('permission', (params || {}).permission)
        .set('pageSize', (params || {}).pageSize)
    });
  }

  getUniqueUsers(permissions, whoAmI) {
    let uniqUsers = {};

    function createUserName(user) {
      let name = "";
      if (user.id.length > 16) {
        name += (user.id.substring(0, 16) + "...");
      } else {
        name += user.id;
      }
      name += (" (" + (user.domain === "local" ? "couchbase" : user.domain) + ")");
      return name;
    }
    uniqUsers[whoAmI.domain + whoAmI.id] = createUserName(whoAmI);
    permissions.forEach(function (permission) {
      permission.users.forEach(user => uniqUsers[user.domain + user.id] = createUserName(user));
    });

    return Object.values(uniqUsers);
  }
}
