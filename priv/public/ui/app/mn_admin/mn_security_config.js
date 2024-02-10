/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import mnUserRoles from "./mn_user_roles_controller.js";
import mnRedaction from "./mn_redaction_controller.js";
import mnCertificates from "./mn_certificates_controller.js";
import mnRolesGroups from "./mn_roles_groups_controller.js";
import mnElementCrane from "../components/directives/mn_element_crane/mn_element_crane.js";
import mnPluggableUiRegistry from "../components/mn_pluggable_ui_registry.js";

import mnSecurityTemplate from "./mn_security.html";
import mnRolesTemplate from "./mn_roles.html";
import mnUserRolesTemplate from "./mn_user_roles.html";
import mnRolesGroupsTemplate from "./mn_roles_groups.html";
import mnCertificatesTemplate from "./mn_certificates.html";

export default 'mnSecurity';

angular
  .module('mnSecurity', [
    mnUserRoles,
    mnRedaction,
    mnCertificates,
    mnRolesGroups,
    mnElementCrane,
    mnPluggableUiRegistry
  ])
  .config(["$stateProvider", mnSecurityConfig])
  .controller("mnSecurityController", ["poolDefault", mnSecurityController]);

function mnSecurityController(poolDefault) {
  var vm = this;
  vm.poolDefault = poolDefault;
}

function mnSecurityConfig($stateProvider) {
  $stateProvider
    .state('app.admin.security', {
      abstract: true,
      url: "/security",
      views: {
        "main@app.admin": {
          controller: "mnSecurityController as securityCtl",
          template: mnSecurityTemplate
        }
      },
      data: {
        permissions: "cluster.admin.security.read",
        title: "Security"
      }
    })
    .state('app.admin.security.roles', {
      abstract: true,
      template: mnRolesTemplate,
      controller: "mnRolesController as rolesCtl"
    })
    .state('app.admin.security.roles.user', {
      url: "/userRoles?openedUsers&startFrom&startFromDomain&sortBy&order&substr&{pageSize:int}",
      params: {
        openedUsers: {
          array: true,
          dynamic: true
        },
        substr: {
          dynamic: true,
          value: ""
        },
        pageSize: {
          value: 20
        },
        startFrom: {
          value: null
        },
        startFromDomain: {
          value: null
        },
        sortBy: {
          value: "id",
          dynamic: true
        },
        order: {
          value: "asc",
          dynamic: true
        }
      },
      controller: "mnUserRolesController as userRolesCtl",
      template: mnUserRolesTemplate
    })
    .state('app.admin.security.roles.groups', {
      url: "/rolesGroups?startFrom&sortBy&order&substr&{pageSize:int}",
      params: {
        openedRolesGroups: {
          array: true,
          dynamic: true
        },
        substr: {
          dynamic: true,
          value: ""
        },
        pageSize: {
          value: 20
        },
        startFrom: {
          value: null
        },
        sortBy: {
          value: "id",
          dynamic: true
        },
        order: {
          value: "asc",
          dynamic: true
        }
      },
      controller: "mnRolesGroupsController as rolesGroupsCtl",
      template: mnRolesGroupsTemplate,
      data: {
        enterprise: true
      }
    })
    .state('app.admin.security.certificate', {
      url: '/certificate',
      controller: 'mnCertController as certCtl',
      template: mnCertificatesTemplate,
      data: {
        enterprise: true
      }
    });
}
