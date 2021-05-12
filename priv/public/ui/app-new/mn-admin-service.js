/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.services = mn.services || {};
mn.services.MnAdmin = (function (Rx) {
  "use strict";

  // counterpart of ns_heart:effective_cluster_compat_version/0
  function encodeCompatVersion(major, minor) {
    return (major < 2) ? 1 : major * 0x10000 + minor;
  }

  function getPoolsDefaultInterval(state) {
    return state.to().name === "app.admin.overview" ? 3000 : 10000;
  }

  //TODO chech that the streams do not contain privat info after logout
  MnAdminService.annotations = [
    new ng.core.Injectable()
  ];

  MnAdminService.parameters = [
    mn.services.MnHelper,
    window['@uirouter/angular'].UIRouter,
    ng.common.http.HttpClient,
    mn.pipes.MnPrettyVersion,
    mn.services.MnPools
  ];

  MnAdminService.prototype.getVersion = getVersion;
  MnAdminService.prototype.getPoolsDefault = getPoolsDefault;
  MnAdminService.prototype.getWhoami = getWhoami;
  MnAdminService.prototype.postPoolsDefault = postPoolsDefault;

  return MnAdminService;

  function MnAdminService(mnHelperService, uiRouter, http, mnPrettyVersionPipe, mnPoolsService) {
    this.stream = {};
    this.http = http;
    this.stream.etag = new Rx.BehaviorSubject();

    this.stream.enableInternalSettings =
      uiRouter.globals.params$.pipe(Rx.operators.pluck("enableInternalSettings"));

    this.stream.whomi =
      (new Rx.BehaviorSubject()).pipe(
        Rx.operators.switchMap(this.getWhoami.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getPoolsDefault =
      Rx.combineLatest(this.stream.etag,
                       uiRouter.globals.success$.pipe(Rx.operators.map(getPoolsDefaultInterval),
                                                      Rx.operators.distinctUntilChanged()))
      .pipe(Rx.operators.switchMap(this.getPoolsDefault.bind(this)),
            mn.core.rxOperatorsShareReplay(1));

    this.stream.isRebalancing =
      this.stream.getPoolsDefault.pipe(
        Rx.operators.map(R.pipe(R.propEq('rebalanceStatus', 'none'), R.not)),
        Rx.operators.distinctUntilChanged());

    this.stream.isBalanced =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("balanced"),
                                       Rx.operators.distinctUntilChanged());

    this.stream.maxBucketCount =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("maxBucketCount"),
                                       Rx.operators.distinctUntilChanged());

    this.stream.uiSessionTimeout =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("uiSessionTimeout"),
                                        Rx.operators.distinctUntilChanged());

    this.stream.failoverWarnings =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("failoverWarnings"),
                                       Rx.operators.distinctUntilChanged(R.equals),
                                       mn.core.rxOperatorsShareReplay(1));

    this.stream.ldapEnabled =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("ldapEnabled"),
                                       Rx.operators.distinctUntilChanged(),
                                       mn.core.rxOperatorsShareReplay(1));

    this.stream.implementationVersion =
      (new Rx.BehaviorSubject()).pipe(Rx.operators.switchMap(this.getVersion.bind(this)),
                                      Rx.operators.pluck("implementationVersion"),
                                      mn.core.rxOperatorsShareReplay(1));
    this.stream.prettyVersion =
      this.stream.implementationVersion.pipe(
        Rx.operators.map(mnPrettyVersionPipe.transform.bind(mnPrettyVersionPipe)));

    this.stream.thisNode =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("nodes"),
                                       Rx.operators.map(R.find(R.propEq('thisNode', true))));
    this.stream.memoryQuotas =
      this.stream.getPoolsDefault.pipe(
        Rx.operators.withLatestFrom(mnPoolsService.stream.quotaServices),
        Rx.operators.map(mnHelperService.pluckMemoryQuotas.bind(mnHelperService)));

    this.stream.clusterName =
      this.stream.getPoolsDefault.pipe(Rx.operators.pluck("clusterName"));

    this.stream.clusterCompatibility =
      this.stream.thisNode.pipe(Rx.operators.pluck("clusterCompatibility"),
                                Rx.operators.distinctUntilChanged());

    this.stream.prettyClusterCompat =
      this.stream.clusterCompatibility.pipe(Rx.operators.map(function (version) {
        var major = Math.floor(version / 0x10000);
        var minor = version - (major * 0x10000);
        return major.toString() + "." + minor.toString();
      }));

    this.stream.compatVersion51 =
      this.stream.clusterCompatibility.pipe(
        Rx.operators.map(R.flip(R.gte)(encodeCompatVersion(5, 1))));

    this.stream.compatVersion55 =
      this.stream.clusterCompatibility.pipe(
        Rx.operators.map(R.flip(R.gte)(encodeCompatVersion(5, 5))));

    this.stream.isNotCompatMode =
      Rx.combineLatest(this.stream.compatVersion51, this.stream.compatVersion55)
      .pipe(Rx.operators.map(R.all(R.equals(true))));

    this.stream.postPoolsDefault =
      new mn.core.MnPostHttp(this.postPoolsDefault.bind(this)).addSuccess().addError();

  }

  function getVersion() {
    return this.http.get("/versions");
  }

  function getWhoami() {
    return this.http.get('/whoami');
  }

  function getPoolsDefault(params) {
    return this.http.get('/pools/default', {
      params: new ng.common.http.HttpParams()
        .set('waitChange', params[1])
        .set('etag', params[0] || "")
    });
  }

  function postPoolsDefault(data) {
    return this.http.post('/pools/default', data[0], {
      params: new ng.common.http.HttpParams()
        .set("just_validate", data[1] ? 1 : 0)
    });
  }

})(window.rxjs);
