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
mn.components.MnUserRolesItem =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnUserRolesItemComponent, mn.core.MnEventableComponent);

    MnUserRolesItemComponent.annotations = [
      new ng.core.Component({
        selector: "mn-user-roles-item",
        templateUrl: "app-new/mn-user-roles-item.html",
        inputs: [
          "user"
        ],
        changeDetection: ng.core.ChangeDetectionStrategy.OnPush
      })
    ];

    MnUserRolesItemComponent.parameters = [
      mn.services.MnPermissions,
      mn.services.MnUserRoles,
      window['@uirouter/angular'].UIRouter
    ];

    return MnUserRolesItemComponent;

    function MnUserRolesItemComponent(mnPermissionsService, mnUserRolesService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      var userCurrentValue = this.mnOnChanges.pipe(Rx.operators.pluck("user", "currentValue"));

      var userId = userCurrentValue.pipe(Rx.operators.map(function (user) {
        return user.id + user.domain;
      }));

      this.securityWrite =
        mnPermissionsService.createPermissionStream("admin.security!write");

      this.userRoles =
        Rx.combineLatest(
          mnUserRolesService.stream.getRolesByRole,
          userCurrentValue
        ).pipe(
          Rx.operators.map(function (resp) {
            var rolesByRole = resp[0];
            var user = resp[1];
            user.roles.forEach(function (role, index) {
              var roleId = role.role + (role.bucket_name || '');
              user.roles[index].desc = rolesByRole[roleId].desc;
              user.roles[index].name = rolesByRole[roleId].name;
            });
            return user.roles;
          })
        );

      this.detailsHashObserver =
        new mn.core.DetailsHashObserver(
          uiRouter,
          "app.admin.security.userRoles",
          "openedUsers",
          this.mnOnDestroy,
          userId
        );
    }

  })(window.rxjs);
