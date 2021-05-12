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
mn.services.MnPermissions = (function (Rx) {
  "use strict";

  var bucketSpecificPermissions = [function (bucket) {
    var name = bucket.name;
    var basePermissions = [
      "cluster.bucket[" + name + "].settings!write",
      "cluster.bucket[" + name + "].settings!read",
      "cluster.bucket[" + name + "].recovery!write",
      "cluster.bucket[" + name + "].recovery!read",
      "cluster.bucket[" + name + "].stats!read",
      "cluster.bucket[" + name + "]!flush",
      "cluster.bucket[" + name + "]!delete",
      "cluster.bucket[" + name + "]!compact",
      "cluster.bucket[" + name + "].xdcr!read",
      "cluster.bucket[" + name + "].xdcr!write",
      "cluster.bucket[" + name + "].xdcr!execute",
      "cluster.bucket[" + name + "].n1ql.select!execute",
      "cluster.bucket[" + name + "].n1ql.index!read",
      "cluster.bucket[" + name + "].n1ql.index!write"
    ];
    if (bucket.name === "." || (bucket.bucketType === "membase")) {
      basePermissions = basePermissions.concat([
        "cluster.bucket[" + name + "].views!read",
        "cluster.bucket[" + name + "].views!write",
        "cluster.bucket[" + name + "].views!compact"
      ]);
    }
    if (bucket.name === "." || (bucket.bucketType !== "memcached")) {
      basePermissions = basePermissions.concat([
        "cluster.bucket[" + name + "].data!write",
        "cluster.bucket[" + name + "].data!read",
        "cluster.bucket[" + name + "].data.docs!read",
        "cluster.bucket[" + name + "].data.docs!upsert"
      ]);
    }

    return basePermissions
  }];

  var interestingPermissions = [
    "cluster.buckets!create",
    "cluster.nodes!write",
    "cluster.pools!read",
    "cluster.server_groups!read",
    "cluster.server_groups!write",
    "cluster.settings!read",
    "cluster.settings!write",
    "cluster.stats!read",
    "cluster.tasks!read",
    "cluster.settings.indexes!read",
    "cluster.admin.internal!all",
    "cluster.xdcr.settings!read",
    "cluster.xdcr.settings!write",
    "cluster.xdcr.remote_clusters!read",
    "cluster.xdcr.remote_clusters!write",
    "cluster.admin.security!read",
    "cluster.admin.logs!read",
    "cluster.admin.settings!read",
    "cluster.admin.settings!write",
    "cluster.logs!read",
    "cluster.pools!write",
    "cluster.settings.indexes!write",
    "cluster.admin.security!write",
    "cluster.samples!read",
    "cluster.nodes!read"
  ];

  interestingPermissions =
    interestingPermissions.concat(generateBucketPermissions({name: "."}));

  MnPermissionsService.annotations = [
    new ng.core.Injectable()
  ];

  MnPermissionsService.parameters = [
    ng.common.http.HttpClient,
    mn.services.MnBuckets,
    mn.services.MnAdmin
  ];

  MnPermissionsService.prototype.getAll = getAll;
  MnPermissionsService.prototype.set = set;
  MnPermissionsService.prototype.setBucketSpecific = setBucketSpecific;
  MnPermissionsService.prototype.doGet = doGet;
  MnPermissionsService.prototype.createPermissionStream = createPermissionStream;

  return MnPermissionsService;

  function MnPermissionsService(http, mnBucketsService, mnAdminService) {
    this.http = http;
    this.stream = {};

    this.stream.url =
      mnAdminService.stream.getPoolsDefault.pipe(
        Rx.operators.pluck("checkPermissionsURI"),
        Rx.operators.distinctUntilChanged()
      );

    this.stream.getBucketsPermissions =
      Rx.combineLatest(
        mnBucketsService.stream.buckets.pipe(
          Rx.operators.map(function (rv) {
            return _.reduce(rv, function (acc, bucket) {
              return acc.concat(generateBucketPermissions(bucket));
            }, []);
          })),
        this.stream.url
      ).pipe(
        Rx.operators.switchMap(this.doGet.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.getSuccess =
      this.stream.url.pipe(
        Rx.operators.map(function (url) {
          return [getAll(), url];
        }),
        Rx.operators.switchMap(this.doGet.bind(this)),
        mn.core.rxOperatorsShareReplay(1)
      );

    this.stream.permissionByBucketNames =
      this.stream.getBucketsPermissions.pipe(
        Rx.operators.map(_.curry(_.reduce)(_, function (rv, value, key, permissions) {
          var splitKey = key.split(/bucket\[|\]/);
          var bucketPermission = splitKey[2];
          var bucketName = splitKey[1];
          if (bucketPermission) {
            rv[bucketPermission] = rv[bucketPermission] || [];
            rv[bucketPermission].push(bucketName);
          }
        }, {}))
      );

  }

  function createPermissionStream(permission, name) {
    if (name instanceof Rx.Observable) {
      return this.stream.getBucketsPermissions.pipe(
        Rx.operators.withLatestFrom(name),
        Rx.operators.map(function (values) {
          return values[0]["cluster.bucket[" + values[1] + "]." + permission];
        }),
        Rx.operators.distinctUntilChanged()
      );
    } else {
      return this.stream.getSuccess.pipe(
        Rx.operators.pluck("cluster." + (name ? ("bucket[" + name + "].") : "") + permission),
        Rx.operators.distinctUntilChanged()
      );
    }
  }

  function generateBucketPermissions(bucketName, buckets) {
    return bucketSpecificPermissions.reduce(function (acc, getChunk) {
      return acc.concat(getChunk(bucketName, buckets));
    }, []);
  }

  function getAll() {
    return _.clone(interestingPermissions);
  }

  function set(permission) {
    if (!_.contains(interestingPermissions, permission)) {
      interestingPermissions.push(permission);
    }
    return this;
  }

  function setBucketSpecific(func) {
    if (angular.isFunction(func)) {
      bucketSpecificPermissions.push(func);
    }
    return this;
  }

  function doGet(urlAndPermissions) {
    return this.http
      .post(urlAndPermissions[1], urlAndPermissions[0].join(',')).pipe(
        Rx.operators.map(function (rv) {
          return JSON.parse(rv);
        })
      );
  }
})(window.rxjs);
