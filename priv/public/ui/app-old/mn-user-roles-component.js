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
mn.components.MnUserRoles =
  (function (Rx) {
    "use strict";

    mn.core.extend(MnUserRoles, mn.core.MnEventableComponent);

    MnUserRoles.annotations = [
      new ng.core.Component({
        templateUrl: "app-new/mn-user-roles.html"
      })
    ];

    MnUserRoles.parameters = [
      mn.services.MnHelper,
      mn.services.MnPools,
      mn.services.MnAdmin,
      mn.services.MnSecurity,
      mn.services.MnUserRoles,
      mn.services.MnPermissions,
      window['@uirouter/angular'].UIRouter
    ];

    MnUserRoles.prototype.filterRouterParams = filterRouterParams;
    MnUserRoles.prototype.trackByFn = trackByFn;

    return MnUserRoles;

    function MnUserRoles(mnHelperService, mnPoolsService, mnAdminService, mnSecurityService, mnUserRolesService, mnPermissionsService, uiRouter) {
      mn.core.MnEventableComponent.call(this);

      var userRolesFieldsGroup = new ng.forms.FormGroup({
        searchTerm: new ng.forms.FormControl(""),
        pageSize: new ng.forms.FormControl()
      });

      this.userRolesFieldsGroup = userRolesFieldsGroup;
      this.onSortByClick = new Rx.BehaviorSubject("id");
      this.isEnterprise = mnPoolsService.stream.isEnterprise;
      this.ldapEnabled = mnAdminService.stream.ldapEnabled;
      this.isSaslauthdAuthEnabled = mnSecurityService.stream.getSaslauthdAuth.pipe(Rx.operators.pluck("enabled"));
      this.securityWrite = mnPermissionsService.createPermissionStream("admin.security!write");

      var routerParams =
          uiRouter.globals.params$.pipe(
            Rx.operators.map(this.filterRouterParams.bind(this)),
            Rx.operators.distinctUntilChanged(_.isEqual)
          );

      var pageSizeFormValue =
          this.userRolesFieldsGroup.valueChanges.pipe(
            Rx.operators.pluck("pageSize"),
            Rx.operators.distinctUntilChanged()
          );

      var searchTermFormValue =
          this.userRolesFieldsGroup.valueChanges.pipe(
            Rx.operators.pluck("searchTerm"),
            Rx.operators.distinctUntilChanged()
          );

      this.users =
        Rx.combineLatest(
          routerParams,
          Rx.timer(0, 10000)
        ).pipe(
          Rx.operators.pluck("0"),
          Rx.operators.switchMap(mnUserRolesService.getUsers.bind(mnUserRolesService)),
          mn.core.rxOperatorsShareReplay(1)
        );

      this.filteredUsers =
        Rx.combineLatest(
          this.users.pipe(Rx.operators.pluck("users")),
          searchTermFormValue
        ).pipe(
          Rx.operators.map(function (resp) {
            return resp[0].filter(listFiter(resp[1]));
          }),
          mnHelperService.sortByStream(this.onSortByClick)
        );

      this.firstPageParams =
        pageSizeFormValue.pipe(
          Rx.operators.map(function (pageSize) {
            return {
              pageSize: pageSize,
              startFromDomain: null,
              startFrom: null
            };
          })
        );

      pageSizeFormValue.pipe(
        Rx.operators.takeUntil(this.mnOnDestroy)
      ).subscribe(function (pageSize) {
        uiRouter.stateService.go('.', {pageSize: pageSize});
      });

      routerParams.pipe(
        Rx.operators.first()
      ).subscribe(function (params) {
        userRolesFieldsGroup.patchValue({pageSize: params.pageSize || 10});
      });
    }

    function listFiter(searchValue) {
      return function (user) {
        var interestingFields = ["id", "name"];
        var l1 = user.roles.length;
        var l2 = interestingFields.length;
        var i1;
        var i2;
        searchValue = searchValue.toLowerCase();
        var role;
        var roleName;
        var rv = false;
        var searchFiled;

        if ((user.domain === "local" ? "Couchbase" : "External")
            .toLowerCase()
            .indexOf(searchValue) > -1) {
          rv = true;
        }

        if (!rv) {
          //look in roles
          loop1:
          for (i1 = 0; i1 < l1; i1++) {
            role = user.roles[i1];
            if (role.role.toLowerCase().indexOf(searchValue) > -1 ||
                (role.bucket_name &&
                 role.bucket_name.toLowerCase().indexOf(searchValue) > -1)) {
              rv = true;
              break loop1;
            }
          }
        }

        if (!rv) {
          //look in interestingFields
          loop2:
          for (i2 = 0; i2 < l2; i2++) {
            searchFiled = interestingFields[i2];
            if (user[searchFiled].toLowerCase().indexOf(searchValue) > -1) {
              rv = true;
              break loop2;
            }
          }
        }

        return rv;
      }
    }

    function trackByFn(user) {
      return user.id + user.domain;
    }

    function filterRouterParams(params) {
      var keys = ["pageSize", "startFromDomain", "startFrom"];
      return _.reduce(keys, function (params1, key) {
        if (params[key]) {
          params1[key] = params[key];
        }
        return params1;
      }, {});
    }

  })(window.rxjs);
