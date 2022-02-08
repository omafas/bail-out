/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

var mn = mn || {};
mn.core = mn.core || {};
mn.core.extend = (function () {
  var extendStatics = Object.setPrototypeOf ||
      ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
      function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
  function __extends(d, b) {
    extendStatics(d, b);
    function __() { this.constructor = d; }
    d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
  }
  return __extends;
})();

mn.core.rxOperatorsShareReplay = (function (Rx) {
  return function () {
    return Rx.pipe(
      Rx.operators.multicast(function () {
        return new Rx.ReplaySubject(1);
      }),
      Rx.operators.refCount());
  }
})(window.rxjs);

mn.core.MnEventableComponent = (function (Rx) {
  var componentLifecycleHooks = [
    "OnChanges",
    "OnInit",
    "DoCheck",
    "AfterContentInit",
    "AfterContentChecked",
    "AfterViewInit",
    "AfterViewChecked",
    "OnDestroy"
  ];
  componentLifecycleHooks.forEach(function (name) {
    if (name === "OnDestroy") {
      MnEventableComponent.prototype["ng" + name] = function (value) {
        this["mn" + name].next();
        this["mn" + name].complete(value);
      }
    } else {
      MnEventableComponent.prototype["ng" + name] = function (value) {
        this["mn" + name].next(value);
      }
    }
  });

  return MnEventableComponent;

  function MnEventableComponent() {
    componentLifecycleHooks.forEach(createSubjects.bind(this));
  }
  function createSubjects(name) {
    //OnChanges triggers before OnInit, so we should keep current value
    this["mn" + name] = (name === "OnChanges") ? new Rx.BehaviorSubject() : new Rx.Subject();
  }
})(window.rxjs);

mn.core.DetailsHashObserver = (function (Rx) {
  DetailsHashObserver.prototype.isOpened = isOpened;
  DetailsHashObserver.prototype.getNewHashValue = getNewHashValue;
  DetailsHashObserver.prototype.prepareHashValue = prepareHashValue;
  DetailsHashObserver.prototype.setNewHashValue = setNewHashValue;

  return DetailsHashObserver;

  function DetailsHashObserver(uiRouter, stateName, hashKey, mnOnDestroy, initialValueStream) {
    this.uiRouter = uiRouter;
    this.hashKey = hashKey;
    this.stateName = stateName;
    this.mnOnDestroy = mnOnDestroy;

    this.stream = {};
    this.stream.toggleDetails = new Rx.Subject();

    this.stream.openedDetailsHash =
      this.uiRouter.globals.params$.pipe(
        Rx.operators.pluck(this.hashKey),
        Rx.operators.distinctUntilChanged(),
        Rx.operators.map(this.prepareHashValue.bind(this)));

    this.stream.isOpened =
      Rx.combineLatest(
        Rx.merge(initialValueStream.pipe(Rx.operators.distinctUntilChanged()),
                 this.stream.toggleDetails),
        this.stream.openedDetailsHash
      )
      .pipe(Rx.operators.map(this.isOpened.bind(this)));

    this.stream.newHashValue =
      this.stream.toggleDetails.pipe(
        Rx.operators.withLatestFrom(this.stream.openedDetailsHash),
        Rx.operators.map(this.getNewHashValue.bind(this)));

    this.stream.newHashValue
      .pipe(Rx.operators.takeUntil(this.mnOnDestroy))
      .subscribe(this.setNewHashValue.bind(this));
  }

  function setNewHashValue(newHashValue) {
    var stateParams = {};
    stateParams[this.hashKey] = newHashValue;
    this.uiRouter.stateService.go(this.stateName, stateParams);
  }

  function prepareHashValue(v) {
    return (v || []).map(decodeURIComponent);
  }

  function getNewHashValue(values) {
    values[1] = _.clone(values[1]);
    if (this.isOpened(values)) {
      return _.difference(values[1], [String(values[0])])
    } else {
      values[1].push(values[0]);
      return values[1];
    }
  }

  function isOpened(values) {
    return values[1].indexOf(values[0]) > -1;
  }

})(window.rxjs);

mn.core.MnPostGroupHttp = (function (Rx) {
  MnPostGroupHttp.prototype.post = post;
  MnPostGroupHttp.prototype.addSuccess = addSuccess;
  MnPostGroupHttp.prototype.addError = addError;
  MnPostGroupHttp.prototype.addLoading = addLoading;
  MnPostGroupHttp.prototype.clearErrors = clearErrors;
  MnPostGroupHttp.prototype.getHttpGroupStreams = getHttpGroupStreams;

  return MnPostGroupHttp;

  function MnPostGroupHttp(httpMap) {
    this.request = new Rx.Subject();
    this.httpMap = httpMap;
  }

  function clearErrors() {
    _.forEach(this.httpMap, function (value, key) {
      value.clearError();
    });
  }

  function addError() {
    this.error =
      Rx.zip.apply(null, this.getHttpGroupStreams("response"))
      .pipe(
        Rx.operators.filter(function (responses) {
          return _.find(responses, function (resp) {
            return resp instanceof ng.common.http.HttpErrorResponse;
          });
        })
      );
    return this;
  }

  function addSuccess() {
    this.success =
      Rx.zip.apply(null, this.getHttpGroupStreams("response"))
      .pipe(
        Rx.operators.filter(function (responses) {
          return !_.find(responses, function (resp) {
            return resp instanceof ng.common.http.HttpErrorResponse;
          });
        })
      );
    return this;
  }

  function post(data) {
    data = data || {};
    this.request.next();
    _.forEach(this.httpMap, function (value, key) {
      value.post(data[key]);
    });
  }

  function getHttpGroupStreams(stream) {
    return _.reduce(this.httpMap, function (result, value, key) {
      result.push(value[stream]);
      return result;
    }, []);
  }

  function addLoading() {
    this.loading =
      Rx.merge(
        Rx.zip.apply(null, this.getHttpGroupStreams("response")).pipe(
          Rx.operators.mapTo(false)
        ),
        this.request.pipe(
          Rx.operators.mapTo(true)
        )
      );
    return this;
  }

})(window.rxjs);

mn.core.MnPostHttp = (function (Rx) {
  MnPostHttp.prototype.addResponse = addResponse;
  MnPostHttp.prototype.addSuccess = addSuccess;
  MnPostHttp.prototype.addLoading = addLoading;
  MnPostHttp.prototype.addError = addError;
  MnPostHttp.prototype.post = post;
  MnPostHttp.prototype.clearError = clearError;

  return MnPostHttp;

  function MnPostHttp(call) {
    this._dataSubject = new Rx.Subject();
    this._errorSubject = new Rx.Subject();
    this._loadingSubject = new Rx.Subject();
    this.addResponse(call);
  }

  function clearError() {
    this._errorSubject.next(null);
  }

  function addResponse(call) {
    this.response =
      this._dataSubject.pipe(
        Rx.operators.switchMap(function (data) {
          return call(data).pipe(Rx.operators.catchError(function (err) {
            return Rx.of(err);
          }));
        }),
        mn.core.rxOperatorsShareReplay(1)
      );
    return this;
  }

  function addError(modify) {
    var error =
        Rx.merge(
          this.response.pipe(
            Rx.operators.switchMap(function (rv) {
              if (rv instanceof ng.common.http.HttpErrorResponse) {
                return Rx.of(rv);
              } else if (mn.services.MnHelper.prototype.isJson(rv) && rv.includes("errors")) {
                return Rx.of(new ng.common.http.HttpErrorResponse({error: rv}));
              } else {
                return Rx.NEVER;
              }
            }),
            Rx.operators.map(R.ifElse(
              R.allPass([
                R.pipe(R.prop("error"), Boolean),
                R.pipe(R.prop("error"), mn.services.MnHelper.prototype.isJson)
              ]),
              R.pipe(R.prop("error"), JSON.parse),
              R.pick(["status"]))),
            mn.core.rxOperatorsShareReplay(1)
          ),
          this._errorSubject
        );
    if (modify) {
      error = error.pipe(modify);
    }
    this.error = error;
    return this;
  }

  function addLoading() {
    this.loading =
      Rx.merge(
        this._loadingSubject,
        this.response.pipe(Rx.operators.mapTo(false))
      );

    return this;
  }

  function addSuccess(modify) {
    var success =
        this.response.pipe(
          Rx.operators.filter(function (rv) {
            return !(rv instanceof ng.common.http.HttpErrorResponse);
          }),
          mn.core.rxOperatorsShareReplay(1)
        );
    if (modify) {
      success = success.pipe(modify);
    }
    this.success = success;
    return this;
  }

  function post(data) {
    this._loadingSubject.next(true);
    this._dataSubject.next(data);
  }
})(window.rxjs);

mn.core.jQueryLikeParamSerializer = (function () {
  jQueryParam.prototype.serializeValue = serializeValue;
  jQueryParam.prototype.serialize = serialize;
  jQueryParam.prototype.toString = toString;

  return jQueryParam;

  //function is borrowed from the Angular source code because we want to
  //use $httpParamSerializerJQLik but with properly encoded params via
  //encodeURIComponent since it uses correct application/x-www-form-urlencoded
  //encoding algorithm, in accordance with
  //https://www.w3.org/TR/html5/forms.html#url-encoded-form-data.
  //And HttpParams doesn't accept array e.g my_key=value1&my_key=value2
  //https://github.com/angular/angular/issues/19071
  function jQueryParam(params) {
    if (!params) {
      return this;
    }
    this.parts = [];
    this.serialize(params, '', true);
  }

  function toString() {
    return this.parts.join("&");
  }

  function serialize(toSerialize, prefix, topLevel) {
    if (_.isArray(toSerialize)) {
      _.forEach(toSerialize, (function (value, index) {
        this.serialize(value, prefix + (_.isObject(value) ? '[' + index + ']' : ''));
      }).bind(this));
    } else if (_.isObject(toSerialize) && !_.isDate(toSerialize)) {
      _.forEach(toSerialize, (function (value, key) {
        this.serialize(value, prefix +
                       (topLevel ? '' : '[') +
                       key +
                       (topLevel ? '' : ']'));
      }).bind(this));
    } else {
      this.parts.push(encodeURIComponent(prefix) + '=' + encodeURIComponent(this.serializeValue(toSerialize)));
    }
  }

  function serializeValue(v) {
    if (_.isObject(v)) {
      return _.isDate(v) ? v.toISOString() : JSON.stringify(v);
    }
    if (v === null || _.isUndefined(v)) {
      return "";
    }
    return v;
  }

})();
