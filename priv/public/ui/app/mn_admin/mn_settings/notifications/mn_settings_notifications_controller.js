(function () {
  "use strict";

  angular
    .module('mnSettingsNotifications', [
      'mnSettingsNotificationsService',
      'mnPromiseHelper',
      'mnSettingsClusterService'
    ])
    .controller('mnSettingsNotificationsController', mnSettingsNotificationsController);

  function mnSettingsNotificationsController($scope, mnPromiseHelper, mnSettingsNotificationsService, pools, mnSettingsClusterService) {
    var vm = this;

    mnSettingsClusterService.registerSubmitCallback(submit);
    vm.implementationVersion = pools.implementationVersion;
    vm.tooltip = 'When this checkbox is enabled, this product automatically collects configuration, usage and performance data, including  cluster information (such as settings and configuration, software version, cluster ID, load levels, and resource quotas), and browser information (such as IP address, inferred geolocation at the city level, and browser type) (collectively with the foregoing, the “Performance Data”). The Performance Data is used by Couchbase to develop and improve our products as well as inform our sales and marketing programs. We do not access or collect any data stored in the Couchbase products. We use this Performance Data to understand aggregate usage patterns and make our products more useful to you. The Performance Data is collected by Couchbase when you access the Admin UI in the configuration wizard if this checkbox is selected. You may turn this feature off at any time by deselecting the checkbox. You can find out more about what data is collected and how it is used if you choose to keep this checkbox enabled <a href="https://docs.couchbase.com/server/current/product-privacy-faq.html">here</a>, which supplements Couchbase’s privacy policy linked <a href="https://www.couchbase.com/privacy-policy">here</a>.';
    activate();

    function activate() {
      mnPromiseHelper(vm, mnSettingsNotificationsService.maybeCheckUpdates())
        .applyToScope("updates");
    }

    function submit() {
      return mnPromiseHelper(vm, mnSettingsNotificationsService.saveSendStatsFlag(vm.updates.enabled))
        .catchGlobalErrors('An error occured, update notifications settings were not saved.')
        .getPromise();
    }
  }
})();
