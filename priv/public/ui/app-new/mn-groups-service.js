var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnGroups = (function (Rx) {
  "use strict";

  MnGroupsService.annotations = [
    new ng.core.Injectable()
  ];

  MnGroupsService.parameters = [
    ng.common.http.HttpClient
  ];

  MnGroupsService.prototype.getServerGroups = getServerGroups;

  return MnGroupsService;

  function MnGroupsService(http) {
    this.http = http;

    this.stream = {};

    this.stream.getServerGroups =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getServerGroups.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

  }

  function getServerGroups() {
    return this.http.get("/pools/default/serverGroups");
  }

})(window.rxjs);
