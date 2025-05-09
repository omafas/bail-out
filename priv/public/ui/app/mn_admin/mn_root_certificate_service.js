/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

export default "mnRootCertificateService";

angular
  .module("mnRootCertificateService", [])
  .factory("mnRootCertificateService", ["$http", mnRootCertificateFactory]);

function mnRootCertificateFactory($http) {
  var mnRootCertificateService = {
    getDefaultCertificate: getDefaultCertificate
  };

  return mnRootCertificateService;

  function getDefaultCertificate() {
    return $http({
      method: 'GET',
      url: '/pools/default/certificate',
      params: {
        extended: true
      }
    }).then(function (resp) {
      return resp.data;
    });
  }
}
