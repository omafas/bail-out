/*
  Copyright 2020-Present Couchbase, Inc.

  Use of this software is governed by the Business Source License included in
  the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
  file, in accordance with the Business Source License, use of this software will
  be governed by the Apache License, Version 2.0, included in the file
  licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {filter, withLatestFrom, pairwise, catchError,
        startWith, switchMap, pluck, takeUntil,
        mapTo, distinctUntilChanged, shareReplay, map} from 'rxjs/operators';
import {BehaviorSubject, Subject, NEVER, of, merge, fromEvent} from 'rxjs';

import {MnHelperService} from './mn.helper.service.js';
import {MnCollectionsService} from './mn.collections.service.js';

import {singletonGuard} from './mn.core.js';

export {MnKeyspaceSelectorService}

class MnKeyspaceSelectorService {
  static get annotations() { return [
    new Injectable()
  ];}

  static get parameters() { return [
    MnHelperService,
    MnCollectionsService
  ];}

  constructor(mnHelperService, mnCollectionsService) {
    singletonGuard(MnKeyspaceSelectorService);
    this.mnHelperService = mnHelperService;
    this.mnCollectionsService = mnCollectionsService;
  }

  createCollectionSelector(options) {
    var filterKey = options.isRolesMode ? "value" : "name";

    var doFocus = new Subject();

    var filters = options.steps.reduce((acc, step) => {
      acc[step] = this.mnHelperService.createFilter(options.component, filterKey);
      return acc;
    }, {});

    var onFocus = options.steps.reduce((acc, step) => {
      acc[step] = new Subject();
      return acc;
    }, {});

    var result = new BehaviorSubject(
      options.steps.reduce((acc, step) => {
        acc[step] = null;
        return acc;
      }, {}));

    var selectionDone =
        result.pipe(filter(v => Object.values(v)
                           .filter(v => !!v).length == options.steps.length));

    var setStepsValuesToFields = () => {
      let value = result.getValue();
      Object.keys(value).forEach(key => {
        if (value[key]) {
          filters[key].group.get("value").setValue(
            value[key] ? value[key][filterKey] : "");
        }
      });
    };

    var mapFocusToStep =
        merge.apply(merge,
                    options.steps.map(step => onFocus[step].pipe(filter(v => v),
                                                                 mapTo(step))));
    var step =
        merge(mapFocusToStep,
              selectionDone.pipe(mapTo("ok")))
        .pipe(shareReplay({refCount: true, bufferSize: 1}));

    var outsideClick = step.pipe(switchMap(v => v != "ok" ?
                                           fromEvent(document, 'click') :
                                           NEVER));

    outsideClick
      .pipe(takeUntil(options.component.mnOnDestroy))
      .subscribe(setStepsValuesToFields);

    var showHideDropdown =
        merge(outsideClick.pipe(mapTo("ok")),
              step)
        .pipe(map(v => v !== "ok"));

    var list = step
        .pipe(distinctUntilChanged(),
              withLatestFrom(result),
              switchMap(options.isRolesMode ?
                        rolesPayload.bind(this) :
                        httpPayload.bind(this)),
              shareReplay({refCount: true, bufferSize: 1}));



    function disableFields(index) {
      options.steps.slice(index).forEach(step => {
        filters[step].group.get("value").disable();
      });
    }

    function setFieldsValues(index, child, value) {
      options.steps.slice(index).forEach(step => {
        value[step] = child;
        filters[step].group.get("value").setValue(child ? child.value : "");
      });
    }

    function getStepList([step, g]) {
      switch (step) {
      case "bucket":
        return options.buckets || this.mnCollectionsService.stream.collectionBuckets;
      case "scope":
        return g.bucket ?
          this.mnCollectionsService.getManifest(g.bucket.name)
          .pipe(pluck("scopes"),
                catchError(() => of([]))) : of([]);
      case "collection":
        return of(g.scope.collections);
      case "ok":
        return NEVER;
      }
    }

    function httpPayload([step, g]) {
      let rv = getStepList.bind(this)([step, g]);

      if (step !== "ok") {
        return rv.pipe(filters[step].pipe);
      } else {
        return rv;
      }
    }

    function rolesPayload([step, g]) {
      let rv;
      switch (step) {
      case "bucket":
        rv = of([{value: "*"},
                 ...options.buckets]);
        break;
      case "scope":
        rv = of([{value: "*"},
                 ...(g.bucket.children ? g.bucket.children[step + "_name"] : [])]);
        break;
      case "collection":
        rv = of([{value: "*"},
                 ...(g.scope.children ? g.scope.children[step + "_name"] : [])]);
        break;
      case "ok":
        return NEVER;
      }
      return rv.pipe(filters[step].pipe);
    }

    function setResultItem(item, step) {
      let value = result.getValue();
      let currentIndex = options.steps.indexOf(step);
      let currentValue = item[filterKey];
      let nextStep = options.steps[currentIndex + 1];
      if (currentValue == "*") {
        disableFields(currentIndex + 1);
        setFieldsValues(currentIndex + 1, {value: "*"}, value);
        value[step] = item;
        result.next(value);
      } else {
        disableFields(currentIndex + 1);
        setFieldsValues(currentIndex + 1, null, value);
        value[step] = item;
        result.next(value);
        if (nextStep) {
          filters[nextStep].group.get("value").enable();
          doFocus.next(nextStep);
        }
      }
    }

    function reset() {
      var next = {};
      disableFields(1);
      setFieldsValues(0, null, next);
      result.next(next);
    }

    function setKeyspace(setVals) {
      let next = {};

      if (setVals.bucket) {
        next["bucket"] = {name: setVals["bucket"]};
        filters["scope"].group.get("value").enable();
      }
      if (setVals.scope) {
        next["scope"] = {name: setVals["scope"]};
      }

      result.next(next);
      setStepsValuesToFields();
    }


    disableFields(1);

    step
      .pipe(startWith(""),
            pairwise(),
            withLatestFrom(result),
            takeUntil(options.component.mnOnDestroy))
      .subscribe(([[prevStep, step], result]) => {
        if (prevStep && prevStep !== "ok") {
          let value = result[prevStep] && result[prevStep][filterKey];
          filters[prevStep].group.get("value").setValue(value);
        }
        if (step && step !== "ok") {
          filters[step].group.get("value").setValue("");
        }
      });

    return {
      setKeyspace: setKeyspace.bind(this),
      reset,
      filters,
      filterKey,
      options,
      setResultItem,
      stream: {
        step,
        list,
        result,
        doFocus,
        onFocus,
        showHideDropdown
      }
    };
  }
}
