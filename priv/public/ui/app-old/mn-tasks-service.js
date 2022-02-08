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
mn.services.MnTasks = (function (Rx) {
  "use strict";

  MnTasksService.annotations = [
    new ng.core.Injectable()
  ];

  MnTasksService.parameters = [
    ng.common.http.HttpClient,
    mn.services.MnAdmin
  ];

  MnTasksService.prototype.get = get;

  return MnTasksService;

  function MnTasksService(http, mnAdminService) {
    this.http = http;
    this.stream = {};
    this.stream.interval = new Rx.Subject();

    var tasksTypesToDisplay = {
      indexer: true,
      rebalance: true,
      orphanBucket: true,
      global_indexes: true,
      view_compaction: true,
      bucket_compaction: true,
      loadingSampleBucket: true,
      clusterLogsCollection: true
    };

    this.stream.updateTasks = new Rx.BehaviorSubject();

    var setupInterval =
        this.stream.interval.pipe(Rx.operators.startWith(0),
                                  Rx.operators.switchMap(function (interval) {
                                    return Rx.timer(interval);
                                  }));
    var getUrl =
        mnAdminService.stream.getPoolsDefault.pipe(Rx.operators.pluck("tasks", "uri"),
                                                   Rx.operators.distinctUntilChanged());

    this.stream.getSuccess =
      Rx.combineLatest(getUrl, setupInterval, this.stream.updateTasks)
      .pipe(Rx.operators.switchMap(this.get.bind(this)),
            mn.core.rxOperatorsShareReplay(1));

    var tasks = this.stream.getSuccess;

    this.stream.tasksWarmingUp =
      tasks.pipe(Rx.operators.map(R.filter(R.allPass([R.propEq("type", "warming_up"),
                                                      R.propEq("status", "running")]))));
    this.stream.isLoadingSampleBucket =
      tasks.pipe(Rx.operators.map(
        R.pipe(R.find(R.allPass([R.propEq("type", "loadingSampleBucket"),
                                 R.propEq("status", "running")])), Boolean)),
                 Rx.operators.distinctUntilChanged());

    this.stream.tasksBucketCompaction =
      tasks.pipe(Rx.operators.map(R.filter(R.propEq("type", "bucket_compaction"))));

    this.stream.tasksRecovery =
      tasks.pipe(Rx.operators.map(R.find(R.propEq("type", "recovery"))));

    this.stream.tasksXDCR =
      tasks.pipe(Rx.operators.map(R.filter(R.propEq("type", "xdcr"))));

    this.stream.isSubtypeGraceful =
      tasks.pipe(Rx.operators.map(R.find(R.allPass([R.propEq("subtype", "gracefulFailover"),
                                                    R.propEq("type", "rebalance")]))));
    this.stream.isRecoveryMode =
      this.stream.tasksRecovery.pipe(Rx.operators.map(Boolean),
                                     Rx.operators.distinctUntilChanged());

    this.stream.tasksRebalance =
      tasks.pipe(Rx.operators.map(R.find(R.propEq("type", "rebalance"))));

    this.stream.extractNextInterval =
      this.stream.getSuccess.pipe(
        Rx.operators.map(function (tasks) {
          return (_.chain(tasks)
                  .pluck('recommendedRefreshPeriod')
                  .compact()
                  .min()
                  .value() * 1000) >> 0 || 10000;
        })
      );

    this.stream.running =
      this.stream.getSuccess.pipe(Rx.operators.map(R.filter(R.propEq("status", "running"))));

    this.stream.tasksToDisplay =
      this.stream.running.pipe(Rx.operators.map(
        R.filter(R.pipe(R.path(["type"]), R.flip(R.prop)(tasksTypesToDisplay)))));

    this.stream.isOrphanBucket =
      this.stream.getSuccess.pipe(Rx.operators.map(
        R.pipe(R.find(R.propEq("type", "orphanBucket")), Boolean)));

  }

  function get(url) {
    return this.http.get(url[0]);
  }

})(window.rxjs);
