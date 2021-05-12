/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { BehaviorSubject, Subject } from '../web_modules/rxjs.js';

export { MnLifeCycleHooksToStream };

let componentLifecycleHooks = [
  "OnChanges",
  "OnInit",
  "DoCheck",
  "AfterContentInit",
  "AfterContentChecked",
  "AfterViewInit",
  "AfterViewChecked",
  "OnDestroy"
];

class MnLifeCycleHooksToStream {
  constructor() {
    componentLifecycleHooks.forEach((name) => {
      //OnChanges triggers before OnInit, so we should keep current value
      this["mn" + name] = (name === "OnChanges") ? new BehaviorSubject() : new Subject();
    });
  }
}

componentLifecycleHooks.forEach(function (name) {
  if (name === "OnDestroy") {
    MnLifeCycleHooksToStream.prototype["ng" + name] = function (value) {
      this["mn" + name].next();
      this["mn" + name].complete(value);
    }
  } else {
    MnLifeCycleHooksToStream.prototype["ng" + name] = function (value) {
      this["mn" + name].next(value);
    }
  }
});
