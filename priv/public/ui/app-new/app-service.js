var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnApp = (function (Rx) {
  "use strict";

  MnApp.annotations = [
    new ng.core.Injectable()
  ];

  return MnApp;

  function MnApp() {
    this.stream = {};
    this.stream.loading = new Rx.BehaviorSubject(false);
    this.stream.httpResponse = new Rx.Subject();
    this.stream.pageNotFound = new Rx.Subject();
    this.stream.appError = new Rx.Subject();
    this.stream.http401 =
      this.stream.httpResponse.pipe(
        Rx.operators.filter(function (rv) {
          //rejection.config.url !== "/controller/changePassword"
          //$injector.get('mnLostConnectionService').getState().isActivated
          return (rv instanceof ng.common.http.HttpErrorResponse) &&
            (rv.status === 401) && !rv.headers.get("ignore-401");
        })
      );
  }
})(window.rxjs);
