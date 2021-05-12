/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import _ from "/ui/web_modules/lodash.js";

export default mnDocumentsListController;

function mnDocumentsListController($rootScope, mnDocumentsListService, $state, $uibModal, removeEmptyValueFilter, jQueryLikeParamSerializerFilter) {
  var vm = this;

  vm.lookupSubmit = lookupSubmit;
  vm.showCreateDialog = showCreateDialog;
  vm.deleteDocument = deleteDocument;
  vm.onFilterClose = onFilterClose;
  vm.onFilterReset = onFilterReset;
  vm.getDocumentsURI = getDocumentsURI;
  vm.getFilterParamsAsString = getFilterParamsAsString;

  var filterConfig = {};

  try {
    filterConfig.params = JSON.parse($state.params.documentsFilter);
  } catch (e) {
    filterConfig.params = {};
  }

  filterConfig.items = {
    inclusiveEnd: true,
    endkey: true,
    startkey: true
  };

  vm.filterConfig = filterConfig;

  activate();

  function getDocumentsURI() {
    return mnDocumentsListService.getDocumentsURI($state.params) + getFilterParamsAsString($state.params);
  }

  function getFilterParamsAsString(params) {
    return "?" + jQueryLikeParamSerializerFilter(removeEmptyValueFilter(mnDocumentsListService.getDocumentsParams($state.params)));
  }

  function onFilterReset() {
    vm.filterConfig.params = {};
  }
  function lookupSubmit() {
    vm.lookupId && $state.go('^.^.editing', {
      documentId: vm.lookupId
    });
  }
  function deleteDocument(documentId) {
    return $uibModal.open({
      controller: 'mnDocumentsDeleteDialogController as documentsDeleteDialogCtl',
      templateUrl: 'app/mn_admin/mn_documents_delete_dialog.html',
      resolve: {
        documentId: function () {
          return documentId;
        }
      }
    });
  }
  function showCreateDialog() {
    return $uibModal.open({
      controller: 'mnDocumentsCreateDialogController as documentsCreateDialogCtl',
      templateUrl: 'app/mn_admin/mn_documents_create_dialog.html',
      resolve: {
        doc: function () {
          return false;
        }
      }
    });
  }
  function onFilterClose() {
    var params = filterConfig.params;
    params = removeEmptyValueFilter(params);
    params && $state.go('^.list', {
      documentsFilter: _.isEmpty(params) ? null : JSON.stringify(params)
    });
  }
  function activate() {
    $rootScope.$broadcast("reloadDocumentsPoller");
  }
}
