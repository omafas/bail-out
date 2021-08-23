/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '/ui/web_modules/@angular/core.js';
import {BehaviorSubject, combineLatest, zip} from '/ui/web_modules/rxjs.js';
import {scan, map, shareReplay, distinctUntilChanged,
        debounceTime, startWith, pluck, takeUntil, tap,
        withLatestFrom} from '/ui/web_modules/rxjs/operators.js';
import {not, sort, prop, descend, ascend, equals} from '/ui/web_modules/ramda.js';
import {FormBuilder} from "/ui/web_modules/@angular/forms.js";
import {UIRouter} from "/ui/web_modules/@uirouter/angular.js";
import ipaddr from "/ui/web_modules/ipaddr.js";

export {MnHelperService};

class MnHelperService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    FormBuilder,
    UIRouter
  ]}

  static mnLocation() {
    let mnLocation = Object.assign({}, window.location);
    let justHostname = mnLocation.hostname;
    if (justHostname.startsWith("[") &&
        justHostname.endsWith("]")) {
      justHostname = justHostname.slice(1, -1);
    }
    try {
      let ipAddr = ipaddr.parse(justHostname);
      mnLocation.kind = ipAddr.kind();
    } catch (e) {
    }
    return mnLocation;
  }

  constructor(formBuilder, uiRouter) {
    this.formBuilder = formBuilder;
    this.uiRouter = uiRouter;
  }

  get daysOfWeek() {
    return ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday']
  }

  get IEC() {
    return {Ki: 1024, Mi: 1024 * 1024, Gi: 1024 * 1024 * 1024}
  }

  invert(v) { //TODO: sould be replaced with Ramda.not
    return !v;
  }

  isJson(str) {
    try {
      JSON.parse(str);
    } catch (e) {
      return false;
    }
    return true;
  }

  calculateMaxMemorySize(totalRAMMegs) {
    return Math.floor(Math.max(totalRAMMegs * 0.8, totalRAMMegs - 1024));
  }

  pluckMemoryQuotas(source) {
    return source[1].reduce((acc, service) => {
      acc[service] = source[0][this.getServiceQuotaName(service)];
      return acc;
    }, {});
  }

  getServiceQuotaName(service) {
    switch (service) {
    case "kv": return "memoryQuota";
    default: return service + "MemoryQuota";
    }
  }

  getServiceVisibleName(service) {
    switch (service) {
    case "kv": return "Data";
    case "index": return "Index";
    case "fts": return "Search";
    case "n1ql": return "Query";
    case "eventing": return "Eventing";
    case "cbas": return "Analytics";
    case "backup": return "Backup";
    }
  }

  validateEqual(key1, key2, erroName) {
    return function (group) {
      if (group.get(key1).value !== group.get(key2).value) {
        var rv = {};
        rv[erroName] = true;
        return rv;
      }
    }
  }

  createToggle(defaultValue) {
    var click = new BehaviorSubject(defaultValue);
    return {
      click: click,
      state: click.pipe(scan(not, true),
                        shareReplay({refCount: true, bufferSize: 1}))
    };
  }

  createSorter(defaultValue) {
    var toggler = this.createToggle(defaultValue);
    var click = toggler.click.pipe(distinctUntilChanged());
    return {
      click: click,
      state: toggler.state,
      pipe: (arrayStream) => {
        return combineLatest(arrayStream, zip(toggler.click, toggler.state))
          .pipe(map(([array, [sortByValue, isDesc]]) => {
            var ascOrDesc = isDesc ? descend : ascend;
            return sort(ascOrDesc(prop(sortByValue)), array);
          }), shareReplay({refCount: true, bufferSize: 1}));
      }
    };
  }

  createFilter(component, filterKey) {
    filterKey = filterKey || "name";
    var group = this.formBuilder.group({value: ""});
    var hotGroup = new BehaviorSubject("");

    group.get("value").valueChanges
      .pipe(takeUntil(component.mnOnDestroy))
      .subscribe(hotGroup);

    var filterFunction = ([list, filterValue]) =>
        list ? list.filter(item => {
          if (typeof item === 'string' || typeof item === 'number') {
            return item.toString().includes(filterValue);
          } else {
            return item[filterKey].includes(filterValue);
          }
        }) : [];

    // R.filter(R.compose(R.any(R.contains(val)), R.values))

    return {
      group: group,
      pipe: (arrayStream) => {
        return combineLatest(arrayStream, hotGroup.pipe(debounceTime(200)))
          .pipe(map(filterFunction),
                shareReplay({refCount: true, bufferSize: 1}));
      }
    };
  }

  createPagenator(component, arrayStream, stateParam, perItem, ajsScope, defaultPageSize) {
    var paramsToExport = new BehaviorSubject();

    var group = this.formBuilder.group({size: null, page: null});

    var setParamToGroup = (page) =>
        group.patchValue(page);

    var setParamToExport = (page) =>
        paramsToExport.next(page);

    var setParamsToUrl = (params) => {
      this.uiRouter.stateService.go('.', params, {notify: false});
    };

    var cloneStateParams = (params) =>
        Object.assign({}, params);

    var getPage = ([array, {size, page}]) =>
        array.slice((page-1) * size, (page-1) * size + size);

    var packPerItemPaginationUrlParams = ([page, currentParams]) => {
      var rv = {};
      currentParams = cloneStateParams(currentParams);
      currentParams[perItem + "s"] = page ? page.size : null;
      currentParams[perItem + "p"] = page ? page.page : null;
      rv[stateParam] = currentParams;
      return rv;
    };

    var unpackPerItemPaginationParams = (page) => ({
      size: page[perItem + "s"] || defaultPageSize || 10,
      page: page[perItem + "p"] || 1
    });

    var packPerPageUrlParams = ([page, currentParams]) => {
      var rv = {};
      rv[stateParam] = page ? Object.assign(cloneStateParams(currentParams), page) : null;
      return rv;
    };

    var rawUrlParam =
        this.uiRouter.globals.params$.pipe(pluck(stateParam));

    var urlParam = rawUrlParam.pipe(perItem ? map(unpackPerItemPaginationParams) : tap(),
                                    distinctUntilChanged(equals),
                                    shareReplay({refCount: true, bufferSize: 1}));

    group.valueChanges
      .pipe(withLatestFrom(rawUrlParam),
            map(perItem ? packPerItemPaginationUrlParams : packPerPageUrlParams),
            takeUntil(component.mnOnDestroy))
      .subscribe(setParamsToUrl);

    urlParam
      .pipe(takeUntil(component.mnOnDestroy))
      .subscribe(setParamToGroup);

    urlParam
      .pipe(takeUntil(component.mnOnDestroy))
      .subscribe(setParamToExport);

    var page = combineLatest(arrayStream, urlParam)
        .pipe(map(getPage), shareReplay({refCount: true, bufferSize: 1}));

    if (ajsScope) {
      page
        .pipe(takeUntil(component.mnOnDestroy))
        .subscribe(page => {
          ajsScope.paginatorPage = page;
        });
      paramsToExport
        .pipe(takeUntil(component.mnOnDestroy))
        .subscribe(values => {
          ajsScope.paginatorValues = Object.assign({}, values);
        });
    }

    return {
      group: group,
      //unfortunly angular valueChanges is a cold observer, BehaviorSubject makes them hot
      //https://github.com/angular/angular/issues/15282
      values: paramsToExport,
      page: page
    };
  }
}
// var mn = mn || {};
// mn.helper = mn.helper || {};
// mn.helper.createBucketTypePipe = (function () {
//   "use strict";

//   return function (bucketType) {
//     var capitalize = bucketType.charAt(0).toUpperCase() + bucketType.slice(1);

//     MnBucketType.annotations = [
//       new ng.core.Pipe({
//         name: "mnIs" + capitalize
//       })
//     ];

//     MnBucketType.prototype.transform = transform;

//     return MnBucketType;

//     function MnBucketType() {
//     }

//     function transform(bucket) {
//       if (bucket instanceof ng.forms.FormGroup) {
//         return bucket.get("bucketType").value === bucketType;
//       } else if (bucket instanceof Object) {
//         return bucket.bucketType === bucketType;
//       } else {
//         return bucket === bucketType;
//       }
//     }

//   }
  // })();
  // mn.pipes.MnIsMembase = mn.helper.createBucketTypePipe("membase")
// mn.pipes.MnIsEphemeral = mn.helper.createBucketTypePipe("ephemeral");
// mn.pipes.MnIsMemcached = mn.helper.createBucketTypePipe("memcached");
