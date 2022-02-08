/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {Subject, BehaviorSubject, combineLatest, zip, fromEvent} from 'rxjs';
import {scan, map, shareReplay, distinctUntilChanged,
        debounceTime, pluck, takeUntil, tap,
        withLatestFrom, startWith, pairwise} from 'rxjs/operators';
import {not, sort, prop, descend, ascend, equals} from 'ramda';
import {FormBuilder} from '@angular/forms';
import {UIRouter} from '@uirouter/angular';
import ipaddr from 'ipaddr';
import CodeMirror from 'codemirror';

import {singletonGuard} from './mn.core.js';

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
    singletonGuard(MnHelperService);

    this.formBuilder = formBuilder;
    this.uiRouter = uiRouter;
  }

  get daysOfWeek() {
    return ['Sunday','Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'];
  }

  get IEC() {
    return {Ki: 1024, Mi: 1024 * 1024, Gi: 1024 * 1024 * 1024};
  }

  transformMBToBytes(mb) {
    let mb1 = Number(mb);

    return isNaN(mb1) ? mb : mb * 1024 * 1024;
  }

  transformBytesToMB(bytes) {
    return Math.floor(bytes / (1024 * 1024));
  }

  byteCount(jsonString) {
    return new TextEncoder().encode(jsonString).length;
  }

  generateID() {
    return Math.random().toString(36).substr(2, 9);
  }

  invert(v) { //TODO: sould be replaced with Ramda.not
    return !v;
  }

  stringifyValues(obj)  {
    return Object.keys(obj).filter(v => obj[v]).join(',');
  }

  stringToObject(string) {
    if (typeof string === "string" && string.length) {
      return string.split(",").reduce((acc, key) => {
        acc[key] = true;
        return acc;
      }, {});
    } else {
      return {};
    }
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

  validateEqual(key1, key2, erroName) {
    return function (group) {
      if (group.get(key1).value !== group.get(key2).value) {
        var rv = {};
        rv[erroName] = true;
        return rv;
      }
    }
  }

  createToggle(isDesc) {
    var click = new Subject();
    return {
      click: click,
      state: click.pipe(scan(not, isDesc || false),
        shareReplay({refCount: true, bufferSize: 1}))
    };
  }

  createToggleForSorter(defaultValue, isDesc) {
    var click = new BehaviorSubject(defaultValue);
    return {
      click: click,
      state: click.pipe(startWith(null),
                        pairwise(),
                        scan((toggle, [prevCol, currCol]) => {
                          return (prevCol === currCol) ? !toggle : isDesc;
                        }, isDesc))
    };
  }

  createSorter(defaultValue, isDesc) {
    var toggler = this.createToggleForSorter(defaultValue, isDesc);
    var state = zip(toggler.click, toggler.state).pipe(shareReplay({refCount: true, bufferSize: 1}));

    return {
      click: toggler.click,
      state: state,
      pipe: (arrayStream) => {
        return combineLatest(arrayStream, state)
          .pipe(map(([array, [sortByValue, isDesc]]) => {
            var ascOrDesc = isDesc ? descend : ascend;
            return sort(ascOrDesc(prop(sortByValue)), array);
          }), shareReplay({refCount: true, bufferSize: 1}));
      }
    };
  }

  createFilter(component, filterKey, splitValueBySpace, customValueGetter) {
    filterKey = filterKey || "name";
    var group = this.formBuilder.group({value: ""});
    var hotGroup = new BehaviorSubject("");

    group.get("value").valueChanges
      .pipe(takeUntil(component.mnOnDestroy))
      .subscribe(hotGroup);

    var toLowerString = value =>
        value.toString().toLowerCase();
    var isSubstring = (substring, completeString) =>
        toLowerString(completeString).includes(toLowerString(substring));
    var filterFunction = ([list, filterValue]) =>
        list ? list.filter(listItem => {
          switch (typeof listItem) {
            case 'string':
            case 'number':
              return isSubstring(filterValue, listItem);
            case 'object':
              if (filterKey instanceof Array) {
                let valueGetter = customValueGetter || ((key, value) => value);
                let filterKeys = filterKey.reduce((acc, key) =>
                                                   acc + " " + valueGetter(key, listItem[key]), "");
                let filterValues = splitValueBySpace ? filterValue.split(" ") : [filterValue];
                return filterValues.every(value =>
                                          isSubstring(value, filterKeys));
              } else {
                return isSubstring(filterValue, listItem[filterKey]);
              }
          }
          return false;
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

  createCodeMirror(elementRef, options) {
    let instance = CodeMirror.fromTextArea(elementRef, options);
    let onChange = fromEvent(instance, 'change');

    return { instance, onChange };
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
