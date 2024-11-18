/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import angular from 'angular';

export default 'mnSettingsClusterAllXDCRLogLevelsDialogController';

angular.module('mnSettingsClusterAllXDCRLogLevelsDialogController', []).controller('mnSettingsClusterAllXDCRLogLevelsDialogController', ["$uibModalInstance", "logLevels", "initialLogLevels", mnSettingsClusterAllXDCRLogLevelsDialogController]);

function mnSettingsClusterAllXDCRLogLevelsDialogController($uibModalInstance, logLevels, initialLogLevels) {
  let vm = this;
  vm.logLevels = logLevels;
  vm.initialLogLevels = initialLogLevels;
  vm.XDCRServices = Object.keys(logLevels);
  vm.closeModal = $uibModalInstance.close;
}

