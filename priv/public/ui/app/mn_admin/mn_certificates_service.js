/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";
import _ from "lodash";
import mnPoolDefault from "../components/mn_pool_default.js";

export default 'mnCertificatesService';

angular
  .module("mnCertificatesService", [mnPoolDefault])
  .factory("mnCertificatesService", ["$http", "mnPoolDefault", mnCertificatesFactory]);

function mnCertificatesFactory($http, mnPoolDefault) {
  var mnCertificatesService = {
    getClientCertificateSettings: getClientCertificateSettings,
    postClientCertificateSettings: postClientCertificateSettings,
    getPoolsDefaultTrustedCAs: getPoolsDefaultTrustedCAs,
    deletePoolsDefaultTrustedCAs: deletePoolsDefaultTrustedCAs,
    getNodeCertificateSettings: getNodeCertificateSettings,
    getNodeCertificateSettingsByNode: getNodeCertificateSettingsByNode,
  };

  return mnCertificatesService;

  function getClientCertificateSettings() {
    return $http({
      method: 'GET',
      url: '/settings/clientCertAuth',
    }).then(function (resp) {
      return resp.data;
    });
  }

  function getNodeCertificateSettings() {
    return $http({
      method: 'GET',
      url: '/pools/default/certificates',
    }).then(function (resp) {
      return resp.data;
    });
  }

  function getNodeCertificateSettingsByNode() {
    return mnCertificatesService.getNodeCertificateSettings().then(function (resp) {
      let certificatesByNode = {};

      resp.forEach(cert => {
        if (cert.warnings) {
          let severityValues = cert.warnings.reduce((acc, curr) => {
            acc.push(curr.severity);
            return acc;
          }, []);

          cert.highestSeverity = Math.max(...severityValues);
        }

        certificatesByNode[cert.node] = cert;
      });

      return certificatesByNode;
    });
  }

  function postClientCertificateSettings(data){
    var settings = _.clone(data);
    if (settings.state == 'disable') {
      settings.prefixes = settings.prefixes.filter(function (pref) {
        return !_.isEqual(pref, {delimiter: '', prefix: '', path: ''});
      });
    }
    if (!mnPoolDefault.export.compat.atLeast51) {
      (['delimiter', 'prefix', 'path']).forEach(function (key) {
        if (settings.prefixes[0] && settings.prefixes[0][key]) {
          settings[key] = settings.prefixes[0][key];
        }
      });
      delete settings.prefixes;
    }
    return $http({
      method: 'POST',
      url: '/settings/clientCertAuth',
      mnHttp: {
        isNotForm: mnPoolDefault.export.compat.atLeast51
      },
      data: settings,
    }).then(function (resp) {
      return resp.data;
    });
  }

  function getPoolsDefaultTrustedCAs() {
    return $http({
      method: 'GET',
      url: '/pools/default/trustedCAs'
    }).then(function (resp) {
      return resp.data && resp.data.map(cert => {
        cert.subjectParsed = cert.subject.split(',').reduce((acc, kv) => {
          let kv1 = kv.split("=");
          acc[kv1[0].trim().toLowerCase()] = (kv1[1] && kv1[1].trim());
          return acc;
        }, {});
        return cert;
      });
    });
  }

  function deletePoolsDefaultTrustedCAs(id) {
    return $http({
      method: 'DELETE',
      url: '/pools/default/trustedCAs/' + encodeURIComponent(id)
    });
  }
}
