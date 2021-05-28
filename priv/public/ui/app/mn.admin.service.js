/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from "/ui/web_modules/@angular/core.js";
import {UIRouter} from "/ui/web_modules/@uirouter/angular.js";
import {BehaviorSubject, combineLatest} from "/ui/web_modules/rxjs.js";
import {pluck,
        switchMap,
        shareReplay,
        map,
        distinctUntilChanged,
        withLatestFrom} from "/ui/web_modules/rxjs/operators.js";
import {HttpClient, HttpParams} from '/ui/web_modules/@angular/common/http.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnPrettyVersion} from './mn.pipes.js';
import {MnPoolsService} from './mn.pools.service.js';
import * as R from '/ui/web_modules/ramda.js';
import { MnHttpRequest } from './mn.http.request.js';

export {MnAdminService};

// counterpart of ns_heart:effective_cluster_compat_version/0
function encodeCompatVersion(major, minor) {
  return (major < 2) ? 1 : major * 0x10000 + minor;
}

class MnAdminService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    MnHelperService,
    UIRouter,
    HttpClient,
    MnPrettyVersion,
    MnPoolsService
  ]}

  constructor(mnHelperService, uiRouter, http, mnPrettyVersionPipe, mnPoolsService) {
    this.stream = {};
    this.http = http;
    this.stream.etag = new BehaviorSubject();

    this.stream.enableInternalSettings =
      uiRouter.globals.params$.pipe(pluck("enableInternalSettings"));

    this.stream.whomi =
      (new BehaviorSubject()).pipe(
        switchMap(this.getWhoami.bind(this)),
        shareReplay({refCount: true, bufferSize: 1})
      );

    // this.stream.getPoolsDefault =
    //   this.stream.etag.pipe(switchMap(this.getPoolsDefault.bind(this)),
    //                         shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getPoolsDefault = new BehaviorSubject({
      buckets: {
        uri: "/pools/default/buckets"
      }
    });

    this.stream.isRebalancing =
      this.stream.getPoolsDefault.pipe(
        map(R.pipe(R.propEq('rebalanceStatus', 'none'), R.not)), distinctUntilChanged());

    this.stream.isBalanced =
      this.stream.getPoolsDefault.pipe(pluck("balanced"), distinctUntilChanged());

    this.stream.maxBucketCount =
      this.stream.getPoolsDefault.pipe(pluck("maxBucketCount"), distinctUntilChanged());

    this.stream.uiSessionTimeout =
      this.stream.getPoolsDefault.pipe(pluck("uiSessionTimeout"), distinctUntilChanged());

    this.stream.failoverWarnings =
      this.stream.getPoolsDefault.pipe(pluck("failoverWarnings"),
                                       distinctUntilChanged(R.equals),
                                       shareReplay({refCount: true, bufferSize: 1}));

    this.stream.ldapEnabled =
      this.stream.getPoolsDefault.pipe(pluck("ldapEnabled"),
                                       distinctUntilChanged(),
                                       shareReplay({refCount: true, bufferSize: 1}));

    this.stream.implementationVersion =
      (new BehaviorSubject()).pipe(switchMap(this.getVersion.bind(this)),
                                   pluck("implementationVersion"),
                                   shareReplay({refCount: true, bufferSize: 1}));

    this.stream.majorMinorVersion =
      this.stream.implementationVersion.pipe(
        map(function (implementationVersion) {
          return implementationVersion.split('.').splice(0,2).join('.');
        })
      );

    this.stream.prettyVersion =
      this.stream.implementationVersion.pipe(
        map(mnPrettyVersionPipe.transform.bind(mnPrettyVersionPipe)));

    this.stream.thisNode =
      this.stream.getPoolsDefault.pipe(pluck("nodes"),
                                       map(R.find(R.propEq('thisNode', true))));
    this.stream.memoryQuotas =
      this.stream.getPoolsDefault.pipe(
        withLatestFrom(mnPoolsService.stream.quotaServices),
        map(mnHelperService.pluckMemoryQuotas.bind(mnHelperService)));

    this.stream.clusterName =
      this.stream.getPoolsDefault.pipe(pluck("clusterName"));

    this.stream.clusterCompatibility =
      this.stream.thisNode.pipe(pluck("clusterCompatibility"), distinctUntilChanged());

    this.stream.prettyClusterCompat =
      this.stream.clusterCompatibility.pipe(map(function (version) {
        var major = Math.floor(version / 0x10000);
        var minor = version - (major * 0x10000);
        return major.toString() + "." + minor.toString();
      }));

    this.stream.compatVersion51 =
      this.stream.clusterCompatibility.pipe(map(R.flip(R.gte)(encodeCompatVersion(5, 1))));

    this.stream.compatVersion55 =
      this.stream.clusterCompatibility.pipe(map(R.flip(R.gte)(encodeCompatVersion(5, 5))));

    this.stream.compatVersion65 =
      this.stream.clusterCompatibility.pipe(map(R.flip(R.gte)(encodeCompatVersion(6, 5))));

    this.stream.compatVersion70 =
      this.stream.clusterCompatibility.pipe(map(R.flip(R.gte)(encodeCompatVersion(7, 0))));

    this.stream.isNotCompatMode =
      combineLatest(this.stream.compatVersion51, this.stream.compatVersion55)
      .pipe(map(R.all(R.equals(true))));

    this.stream.postPoolsDefaultValidation =
      new MnHttpRequest(this.postPoolsDefault(true).bind(this)).addSuccess().addError();

    this.stream.postPoolsDefault =
      new MnHttpRequest(this.postPoolsDefault(false).bind(this)).addSuccess().addError();

  }

  getVersion() {
    return this.http.get("/versions");
  }

  getWhoami() {
    return this.http.get('/whoami');
  }

  getPoolsDefault(etag) {
    return this.http.get('/pools/default', {
      params: new HttpParams().set('waitChange', 10000).set('etag', etag || "")
    });
  }

  postPoolsDefault(validate) {
    return function (data) {
      return this.http.post('/pools/default', data, {
        params: new HttpParams().set("just_validate", validate ? 1 : 0)
      });
    }
  }
}
