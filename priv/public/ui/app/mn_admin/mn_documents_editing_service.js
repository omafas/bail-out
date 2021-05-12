/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from "/ui/web_modules/angular.js";
import _ from "/ui/web_modules/lodash.js";
import js_beautify from "/ui/web_modules/js-beautify.js";

import mnFilters from "/ui/app/components/mn_filters.js";

export default "mnDocumentsEditingService";

angular
  .module("mnDocumentsEditingService", [mnFilters])
  .factory("mnDocumentsEditingService", mnDocumentsEditingFactory);

function mnDocumentsEditingFactory($http, $q, getStringBytesFilter, docBytesLimit) {
  var mnDocumentsEditingService = {
    getDocument: getDocument,
    createDocument: createDocument,
    deleteDocument: deleteDocument,
    getDocumentsEditingState: getDocumentsEditingState,
    isJsonOverLimited: isJsonOverLimited
  };

  return mnDocumentsEditingService;

  function isJsonOverLimited(json) {
    return getStringBytesFilter(json) > docBytesLimit;
  }

  function getDocumentsEditingState(params) {
    return getDocument(params).then(function getDocumentState(resp) {
      var doc = resp.data
      var rv = {};
      var editorWarnings = {
        documentIsBase64: ("base64" in doc),
        documentLimitError: isJsonOverLimited(doc.json)
      };
      rv.title = doc.meta.id;
      if (_.chain(editorWarnings).values().some().value()) {
        rv.editorWarnings = editorWarnings;
      } else {
        rv.doc = js_beautify(doc.json, {"indent_size": 2});
        rv.meta = JSON.stringify(doc.meta, null, "  ");
      }
      return rv;
    }, function (resp) {
      switch (resp.status) {
      case 404: return {
        editorWarnings: {
          notFound: true
        },
        title: params.documentId
      };
      default: return {
        errors: resp && resp.data,
      };
      }
    });
  }

  function deleteDocument(params) {
    return $http({
      method: "DELETE",
      url: buildDocumentUrl(params)
    });
  }

  function createDocument(params, doc, flags) {
    return $http({
      method: "POST",
      url: buildDocumentUrl(params),
      data: {
        flags: flags || 0x02000006,
        value: js_beautify(doc, {
          "indent_size": 0,
          "eol": "",
          "remove_space_before_token": true,
          "indent_char": ""}) || '{"click": "to edit", "with JSON": "there are no reserved field names"}'
      }
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
