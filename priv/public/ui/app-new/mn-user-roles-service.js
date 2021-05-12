/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnUserRoles = (function (Rx) {
  "use strict";

  MnUserRolesService.annotations = [
    new ng.core.Injectable()
  ];

  MnUserRolesService.parameters = [
    ng.common.http.HttpClient
  ];

  MnUserRolesService.prototype.getUsers = getUsers;
  MnUserRolesService.prototype.getRoles = getRoles;
  MnUserRolesService.prototype.doRolesByRole = doRolesByRole;

  return MnUserRolesService;

  function MnUserRolesService(http) {
    this.http = http;
    this.stream = {};

    this.stream.getRolesByRole =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getRoles.bind(this)),
        Rx.operators.map(this.doRolesByRole.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );
  }

  function doRolesByRole(roles) {
    var rolesByRole = {};
    roles.forEach(function (role) {
      rolesByRole[role.role + (role.bucket_name || "")] = role;
    });
    return rolesByRole;
  }

  function getRoles() {
    return this.http.get("/settings/rbac/roles");
  }

  function getUsers(params) {
    return this.http.get("/settings/rbac/users", {
      params:  _.reduce(params, function (params1, value, key) {
        return params1.set(key, value);
      }, new ng.common.http.HttpParams())
    }).pipe(
      Rx.operators.map(function (resp) {
        var i;
        for (i in resp.links) {
          resp.links[i] = resp.links[i].split("?")[1]
            .split("&")
            .reduce(function(prev, curr, i, arr) {
              var p = curr.split("=");
              prev[decodeURIComponent(p[0])] = decodeURIComponent(p[1]);
              return prev;
            }, {});
        }
        if (!resp.users) {//in oreder to support compatibility mode
          return {
            users: resp
          };
        } else {
          return resp;
        }
      })
    );
  }

})(window.rxjs);
