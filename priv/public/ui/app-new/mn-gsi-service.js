var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnGSI = (function (Rx) {
  "use strict";

  MnGSI.annotations = [
    new ng.core.Injectable()
  ];

  MnGSI.parameters = [
    ng.common.http.HttpClient
  ];

  MnGSI.prototype.getIndexStatus = getIndexStatus;

  return MnGSI;

  function MnGSI(http) {
    this.http = http;
    this.stream = {};

    this.stream.getIndexStatus =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getIndexStatus.bind(this)),
        mn.core.rxOperatorsShareReplay(1));
  }

  function getIndexStatus() {
    return this.http.get("/indexStatus");
  }

})(window.rxjs);
