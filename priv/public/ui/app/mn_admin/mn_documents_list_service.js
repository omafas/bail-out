/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";

export default "mnDocumentsListService";

angular
  .module("mnDocumentsListService", [])
  .factory("mnDocumentsListService", mnDocumentsListFactory);

function mnDocumentsListFactory($http, $q, docsLimit) {
  var mnDocumentsListService = {
    getDocuments: getDocuments,
    getDocumentsListState: getDocumentsListState,
    getDocumentsParams: getDocumentsParams,
    getDocumentsURI: getDocumentsURI
  };

  return mnDocumentsListService;

  function getListState(docs, params) {
    var rv = {};
    rv.pageNumber = params.pageNumber;
    rv.isNextDisabled = docs.rows.length <= params.pageLimit || params.pageLimit * (params.pageNumber + 1) === docsLimit;
    if (docs.rows.length > params.pageLimit) {
      docs.rows.pop();
    }

    rv.docs = docs;

    rv.pageLimits = [10, 20, 50, 100];
    rv.pageLimits.selected = params.pageLimit;
    return rv;
  }

  function getDocumentsListState(params) {
    return getDocuments(params).then(function (resp) {
      return getListState(resp.data, params);
    }, function (resp) {
      switch (resp.status) {
      case 0:
      case -1: return $q.reject(resp);
      case 404: return !params.commonBucket ? {status: "_404"} : resp;
      case 501: return {};
      default: return resp;
      }
    });
  }

  function getDocumentsParams(params) {
    var param;
    try {
      param = JSON.parse(params.documentsFilter) || {};
    } catch (e) {
      param = {};
    }
    var page = params.pageNumber;
    var limit = params.pageLimit;
    var skip = page * limit;

    param.skip = String(skip);
    param.include_docs = true;
    param.limit = String(limit + 1);

    if (param.startkey) {
      param.startkey = JSON.stringify(param.startkey);
    }

    if (param.endkey) {
      param.endkey = JSON.stringify(param.endkey);
    }
    return param;
  }

  function getDocumentsURI(params) {
    let bucket = params.bucket || params.commonBucket;
    let base = "/pools/default/buckets/" + encodeURIComponent(bucket);
    if (params.scope && params.collection) {
      base += "/scopes/" + encodeURIComponent(params.scope) + "/collections/" + encodeURIComponent(params.collection);
    }
    return base + "/docs";
  }

  function getDocuments(params) {
    return $http({
      method: "GET",
      url: getDocumentsURI(params),
      params: getDocumentsParams(params)
    });
  }
}
