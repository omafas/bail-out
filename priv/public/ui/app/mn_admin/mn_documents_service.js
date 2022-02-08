/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "angular";

export default "mnDocumentsService";

angular
  .module("mnDocumentsService", [])
  .factory("mnDocumentsService", ["$http", "$q", mnDocumentsFactory]);

function mnDocumentsFactory($http, $q) {
  var mnDocumentsService = {
    getDocument: getDocument,
    getDocuments: getDocuments,
    getDocumentsParams: getDocumentsParams,
    getDocumentsURI: getDocumentsURI
  };

  return mnDocumentsService;


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

  // this is used in views editing service
  function getDocuments(params) {
    return $http({
      method: "GET",
      url: getDocumentsURI(params),
      params: getDocumentsParams(params)
    });
  }

  function getDocument(params) {
    if (!params.documentId) {
      return $q.reject({data: {reason: "Document ID cannot be empty"}});
    }
    return $http({
      method: "GET",
      url: buildDocumentUrl(params)
    });
  }
  function buildDocumentUrl(params) {
    let bucket = params.bucket || params.commonBucket;
    let base =  "/pools/default/buckets/" + encodeURIComponent(bucket);
    if (params.scope && params.collection) {
      base += "/scopes/" + encodeURIComponent(params.scope) + "/collections/" + encodeURIComponent(params.collection);
    }
    return base + "/docs/" + encodeURIComponent(params.documentId);
  }
}
