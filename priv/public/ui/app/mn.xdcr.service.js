/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Injectable} from '@angular/core';
import {HttpClient, HttpErrorResponse} from '@angular/common/http';
import {BehaviorSubject, combineLatest, timer, of, NEVER} from 'rxjs';
import {map, shareReplay, switchMap, throttleTime,
        pluck, distinctUntilChanged, delay, retryWhen, catchError} from 'rxjs/operators';
import {pipe, filter, propEq, sortBy, prop, groupBy} from 'ramda';

import {MnStatsService} from "./mn.stats.service.js"
import {MnTasksService} from './mn.tasks.service.js';
import {MnHttpRequest} from './mn.http.request.js';
import {MnPermissions} from './ajs.upgraded.providers.js';

import {singletonGuard} from './mn.core.js';

let collectionDelimiter = ".";

export {MnXDCRService, collectionDelimiter};

class MnXDCRService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnStatsService,
    MnTasksService,
    MnPermissions
  ]}

  constructor(http, mnStatsService, mnTasksService, mnPermissions) {
    singletonGuard(MnXDCRService);

    this.http = http;

    this.stream = {};

    this.stream.updateRemoteClusters =
      new BehaviorSubject();

    this.stream.deleteRemoteClusters =
      new MnHttpRequest(this.deleteRemoteClusters.bind(this))
      .addSuccess()
      .addError();

    this.stream.deleteCancelXDCR =
      new MnHttpRequest(this.deleteCancelXDCR.bind(this))
      .addSuccess()
      .addError();

    this.stream.getSettingsReplications = this.createGetSettingsReplicationsPipe();

    this.stream.postSettingsReplications =
      new MnHttpRequest(this.postSettingsReplications(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postPausePlayReplication =
      new MnHttpRequest(this.postSettingsReplications(false).bind(this))
      .addSuccess()
      .addError();

    this.stream.postSettingsReplicationsValidation =
      new MnHttpRequest(this.postSettingsReplications(true).bind(this))
      .addSuccess(map(parsePostCreateReplicationSuccess))
      .addError();

    this.stream.postXdcrConnectionPreCheck =
      new MnHttpRequest(this.postXdcrConnectionPreCheck.bind(this))
      .addSuccess()
      .addError();

    function parsePostCreateReplicationSuccess(data) {
      //we should parse success response since XDCR
      //warnings returns here
      return JSON.parse(data);
    }

    function extractPostCreateReplicationError(error) {
      return (error && error.errors) || ({_: (error && error.error) || error});
    }

    this.stream.postCreateReplication =
      new MnHttpRequest(this.postCreateReplication.bind(this, false))
      .addSuccess(map(parsePostCreateReplicationSuccess))
      .addError(map(extractPostCreateReplicationError));

    this.stream.postCreateReplicationValidation =
      new MnHttpRequest(this.postCreateReplication.bind(this, true))
      .addSuccess(map(parsePostCreateReplicationSuccess))
      .addError(map(extractPostCreateReplicationError));

    this.stream.postRemoteClusters =
      new MnHttpRequest(this.postRemoteClusters.bind(this))
      .addSuccess()
      .addError();

    this.stream.postRegexpValidation =
      new MnHttpRequest(this.postRegexpValidation.bind(this))
      .addSuccess(map(data => JSON.parse(data)))
      .addError(map(error => ({error: error.error || error})));

    this.stream.postRegexpValidationExpression =
      new MnHttpRequest(this.postRegexpValidation.bind(this))
        .addSuccess(map(data => JSON.parse(data)))
        .addError(map(error => ({error: error.error || error})));

    let doGetRemoteClusters =
        combineLatest(timer(0, 10000),
                      this.stream.updateRemoteClusters)
        .pipe(switchMap(this.getRemoteClusters.bind(this)),
              shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getRemoteClusters = mnPermissions.stream
      .pipe(pluck("cluster", "xdcr", "remote_clusters", "read"),
            distinctUntilChanged(),
            switchMap((canRead) => canRead ? doGetRemoteClusters : NEVER));

    let doGetIncomingSources =
      timer(0, 10000)
      .pipe(switchMap(this.getIncomingSources.bind(this)),
        shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getIncomingSources = mnPermissions.stream
    .pipe(
      distinctUntilChanged(),
      switchMap((canRead) => canRead ? doGetIncomingSources : NEVER),
      map((incomingSources) => (incomingSources || []).sort((s1, s2) => s1.SourceClusterName.localeCompare(s2.SourceClusterName))));

    this.stream.getRemoteClustersFiltered = this.stream.getRemoteClusters
      .pipe(map(pipe(filter(propEq('deleted', false)),
                     sortBy(prop('name')))),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getRemoteClustersByUUID = this.stream.getRemoteClusters
      .pipe(map(groupBy(prop("uuid"))),
            shareReplay({refCount: true, bufferSize: 1}));


    this.stream.getChangesLeftTotal = mnTasksService.stream.tasksXDCR
      .pipe(throttleTime(1000, undefined, {leading: true, trailing: true}),
            map(tasks => tasks && tasks.map(task => ({
              nodesAggregation: "sum",
              applyFunctions: ["sum"],
              start: -5,
              step: 10,
              metric: [
                {label: "name", value: "xdcr_changes_left_total"},
                {label: "sourceBucketName", value: task.source}
              ]
            }))),
            switchMap(configs => {
              if (configs) {
                return mnStatsService.postStatsRange(configs)
                  .pipe(map(stats =>
                            stats.reduce((acc, stat) =>
                                         acc + Number(stat.data[0] &&
                                                      stat.data[0].values[0][1]) || 0, 0)));
              } else {
                return of(0);
              }
            }),
            shareReplay({refCount: true, bufferSize: 1}));

  }

  prepareReplicationSettigns([, isEnterprise, compatVersion55, compatVersion80, filterFormHelper]) {
    //this points to the component view instance
    var settings = Object.assign({}, this.form.group.value, this.filterRegexpGroup.value);
    delete settings.docId;
    if (isEnterprise) {
      settings.filterSkipRestream = (settings.filterSkipRestream === "true");
    } else {
      delete settings.filterExpression;
      delete settings.filterSkipRestream;
      delete settings.priority;
    }

    if (!filterFormHelper.enableFilters) {
      settings.filterExpression = '';
      settings.filterExpiration = false;
      settings.filterDeletion = false;
      settings.filterBypassExpiry = false;
      settings.filterBinary = false;
    }

    if (!this.isEditMode) {
      delete settings.filterSkipRestream;
    }
    if (!isEnterprise || !compatVersion55) {
      delete settings.compressionType;
    }
    if (!isEnterprise) {
      delete settings.networkUsageLimit;
    }
    if (settings.collectionsExplicitMapping) {
      let rules = this.explicitMappingRules.getValue();
      settings.collectionsMigrationMode = false;
      if (Object.keys(rules).length) {
        settings.colMappingRules = JSON.stringify(rules);
      } else {
        settings.collectionsExplicitMapping = false;
      }
    }
    if (settings.collectionsMigrationMode) {
      let rules = this.explicitMappingMigrationRules.getValue();
      settings.collectionsExplicitMapping = false;
      if (Object.keys(rules).length) {
        settings.colMappingRules = JSON.stringify(rules);
      } else {
        settings.collectionsMigrationMode = false;
      }
    }

    // conflict logging
    let conflictLogRules = this.conflictLogRules.getValue();
    // don't send empty values to backend
    if (!conflictLogRules.bucket) {
      delete conflictLogRules.bucket;
    }
    if (!conflictLogRules.collection) {
      delete conflictLogRules.collection;
    }

    if (settings.conflictLogMapping &&
      ((conflictLogRules.loggingRules && Object.keys(conflictLogRules.loggingRules).length) || conflictLogRules.bucket || conflictLogRules.collection)) {
      settings.conflictLogging = JSON.stringify({...conflictLogRules, ...{disabled: !this.conflictLogMappingGroup.rootControls.get('enableConflictLog').value}});
    } else {
      settings.conflictLogging = JSON.stringify({});
    }

    settings.replicationType = "continuous";

    if (!isEnterprise || !compatVersion80) {
      delete settings.mobile;
    } else if (settings.mobile) {
      settings.mobile = 'Active';
    } else {
      settings.mobile = 'Off';
    }

    return settings;
  }

  setMappingRule(sourceFlag, source, target, rules) {
    if (sourceFlag) {
      rules[source] = target;
    } else {
      delete rules[source];
    }
  }

  createGetSettingsReplicationsPipe(id) {
    return (new BehaviorSubject(id)).pipe(
      switchMap(this.getSettingsReplications.bind(this)),
      shareReplay({refCount: true, bufferSize: 1}));
  }

  postRegexpValidation(params) {
    return this.http.post("/_goxdcr/regexpValidation", params);
  }

  deleteRemoteClusters(name) {
    return this.http.delete('/pools/default/remoteClusters/' + encodeURIComponent(name));
  }

  deleteCancelXDCR(id) {
    return this.http.delete('/controller/cancelXDCR/' + encodeURIComponent(id));
  }

  getSettingsReplications(id) {
    return this.http.get("/settings/replications" +
                         (id ? ("/" + encodeURIComponent(id)) : ""));
  }

  postSettingsReplications(validate) {
    return source =>
      this.http.post("/settings/replications" +
                     (source[0] ? ("/" + encodeURIComponent(source[0])) : ""),
                     source[0] ? source[1] : source,
                     {params: {"just_validate": validate ? 1 : 0}});
  }

  postCreateReplication(validate, data) {
    return this.http.post("/controller/createReplication", data, {
      params: {"just_validate": validate ? 1 : 0}
    });
  }

  getRemoteClusters() {
    return this.http.get("/pools/default/remoteClusters");
  }

  getIncomingSources() {
    return this.http.get("/xdcr/sourceClusters");
  }

  prepareRequestBody(source) {
    var cluster = Object.assign({}, source);
    var requestBody = {};
    var requestBodyFields = ["name", "hostname", "username", "password"];

    if (cluster.hostname && !(/^\[?([^\]]+)\]?:(\d+)$/).exec(cluster.hostname)) {
      // ipv4/ipv6/hostname + port
      cluster.hostname += ":8091";
    }

    if (cluster.network_type) {
      requestBodyFields.push("network_type");
    }
    if (cluster.demandEncryption) {
      requestBodyFields.push("demandEncryption");
      requestBodyFields.push("certificate");
      requestBodyFields.push("encryptionType");
      if (cluster.encryptionType === "full") {
        requestBodyFields.push("clientCertificate");
        requestBodyFields.push("clientKey");
      }
    }
    requestBodyFields.forEach(field =>
                              requestBody[field] = cluster[field]);
    return requestBody;
  }

  getXdcrConnectionPreCheck(id) {
    return this.http.get("/xdcr/connectionPreCheck" + (id ? ("?taskId=" + encodeURIComponent(id)) : ""))
      .pipe(switchMap((resp) => {
              if (resp.done) {
                return of(resp);
              } else {
                throw {name: 'retry', result: resp.result};
              }
            }),
            retryWhen((errors) => {
              let count = 1;
              return errors.pipe(
                delay(2000),
                map((error) => {
                  count++;
                  if ((count === 30 && error.name === 'retry') ||
                      (error instanceof HttpErrorResponse)) {
                    throw error;
                  }
                }));
            }),
            catchError((error) => {
              let errorMessage = "";
              if (error.name === 'retry') {
                errorMessage = "The connection check has failed after 30 attempts. Please try again later.";
              } else {
                errorMessage = error.message || error.error || error.statusText;
              }

              let failed = new HttpErrorResponse({});
              failed._ = errorMessage;
              failed.result = error.result || {};
              delete failed.name;
              throw failed;
            }));
  }

  postXdcrConnectionPreCheck(source) {
    let requestBody = this.prepareRequestBody(source[0]);

    return this.http.post("/xdcr/connectionPreCheck", requestBody)
      .pipe(map((resp) => JSON.parse(resp)),
            switchMap((resp) => {
              return this.getXdcrConnectionPreCheck(resp.taskId);
            }))
  }

  postRemoteClusters(source) {
    let name = source[1];
    let requestBody = this.prepareRequestBody(source[0]);

    return this.http.post('/pools/default/remoteClusters' +
                          (name ? ("/" + encodeURIComponent(name)) : ""), requestBody);
  }
}
