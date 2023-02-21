/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/
import {Injectable} from '@angular/core';
import {NgbModal} from '@ng-bootstrap/ng-bootstrap';

import {HttpClient, HttpParams} from '@angular/common/http';
import {pluck, switchMap, concatMap, shareReplay,
  distinctUntilChanged, map, withLatestFrom} from 'rxjs/operators';
import {BehaviorSubject, timer, combineLatest} from 'rxjs';
import {filter, anyPass, allPass, propEq} from 'ramda';

import {singletonGuard} from './mn.core.js'
import {MnAdminService} from './mn.admin.service.js';
import {MnPermissions} from './ajs.upgraded.providers.js';
import {MnHttpRequest} from './mn.http.request.js';
import {MnHelperService} from './mn.helper.service.js';
import {MnSettingsAutoCompactionService} from './mn.settings.auto.compaction.service.js';

export {MnBucketsService};

class MnBucketsService {
  static get annotations() { return [
    new Injectable()
  ]}

  static get parameters() { return [
    HttpClient,
    MnAdminService,
    MnHelperService,
    MnSettingsAutoCompactionService,
    MnPermissions,
    NgbModal
  ]}

  constructor(http, mnAdminService, mnHelperService, mnSettingsAutoCompactionService, mnPermissions, modalService) {
    singletonGuard(MnBucketsService);

    this.stream = {};
    this.http = http;
    this.modalService = modalService;
    this.mnHelperService = mnHelperService;
    this.mnAdminService = mnAdminService;
    this.mnSettingsAutoCompactionService = mnSettingsAutoCompactionService;

    this.stream.bucketsUri = mnAdminService.stream.getPoolsDefault
      .pipe(pluck("buckets", "uri"),
            distinctUntilChanged());

    this.stream.getBuckets = this.stream.bucketsUri
      .pipe(switchMap(this.get.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.updateBucketsPoller = new BehaviorSubject();

    this.stream.getBucketsByName = this.stream.getBuckets
      .pipe(map(buckets =>
                   buckets.reduce((acc, bucket) => {
                     acc[bucket.name] = bucket;
                     return acc;
                   }, {})),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.bucketsMembaseCouchstore = this.stream.getBuckets
      .pipe(map(filter(allPass([propEq('bucketType', 'membase'),
                                propEq('storageBackend', 'couchstore')]))),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.bucketsMembaseEphemeral = this.stream.getBuckets
      .pipe(map(filter(anyPass([propEq('bucketType', 'membase'),
                                propEq('bucketType', 'ephemeral')]))),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.getBucketsPool =
      combineLatest(this.stream.bucketsUri,
                    timer(0, 4000),
                    this.stream.updateBucketsPoller)
      .pipe(map(([url,]) => url),
            concatMap(this.get.bind(this)),
            shareReplay({refCount: true, bufferSize: 1}));

    this.stream.defaultAutoCompactionData = mnSettingsAutoCompactionService.stream.settingsSource;

    this.stream.deleteBucket =
      new MnHttpRequest(this.deleteBucket.bind(this))
        .addSuccess()
        .addError();

    this.stream.flushBucket =
      new MnHttpRequest(this.flushBucket.bind(this))
        .addSuccess()
        .addError();
  }

  isNewBucketAllowed([permissions, maxBucketsCountReached, isRebalancing]) {
    return permissions.cluster.buckets.create && !isRebalancing && !maxBucketsCountReached;
  }

  getNodesCountByStatus(nodes) {
    let nodesByStatuses = {};

    nodes.forEach(node => {
      let status = this.getMessage(node.status, node.clusterMembership);

      if (status) {
        nodesByStatuses[status] = (nodesByStatuses[status] || 0) + 1;
      }
    });

    return nodesByStatuses;
  }

  getMessage(status, clusterMembership) {
    let rvStatus = clusterMembership === 'inactiveFailed' ? 'failed over, ' : '';

    switch (status) {
      case 'unhealthy':
        return rvStatus + 'not responding';
      case 'warmup':
        return rvStatus + 'pending';
      default:
        return rvStatus;
    }
  }

  getNodesCountByStatusMessage(statusCount) {
    return Object.keys(statusCount).map(status => {
      return statusCount[status] + ' node' + (statusCount[status] !== 1 ? "s" : "") + ' ' + status;
    });
  }

  getWarmUpProgress([tasks, bucket]) {
    if (!bucket || !tasks) {
      return false;
    }

    let task = tasks.find(task => task.bucket === bucket.name);
    if (task) {
      if (!(Number(task.stats.ep_warmup_estimated_key_count) ||
        Number(task.stats.ep_warmup_estimated_value_count))) {
        return 0;
      }

      let totalPercent = 0;
      switch (task.stats.ep_warmup_state) {
        case "loading keys":
          totalPercent += (Number(task.stats.ep_warmup_key_count) /
            Number(task.stats.ep_warmup_estimated_key_count) * 100);
          break;
        case "loading data":
          totalPercent += (Number(task.stats.ep_warmup_value_count) /
            Number(task.stats.ep_warmup_estimated_value_count) * 100);
          break;
        default:
          break;
      }

      return totalPercent / bucket.nodes.length;
    }

    return false;
  }

  getNodesStatusClass(nodes) {
    let statusClass = nodes.length ? 'healthy' : 'inactive';

    for (let i = 0; i < nodes.length; i++) {
      let node = nodes[i];
      if (node.status === 'unhealthy') {
        statusClass = 'unhealthy';
        break;
      }
      if (statusClass !== 'inactiveFailed' && node.status === 'warmup') {
        statusClass = 'warmup';
      }
      if (node.clusterMembership === 'inactiveFailed') {
        statusClass = 'inactiveFailed';
      }
    }

    return ('dynamic_' + statusClass);
  }

  getResidentRatio(bucket) {
    let items = bucket.basicStats.itemCount;
    let activeResident = bucket.basicStats.vbActiveNumNonResident;

    if (items === 0) {
      return 100;
    } else if (items < activeResident) {
      return 0;
    }

    return (items - activeResident) * 100 / items;
  }

  getRamConfigParams(bucket) {
    if (!bucket) {
      return;
    }

    let totals = bucket.basicStats.storageTotals.ram;
    return {
      total: totals.quotaTotalPerNode * bucket.nodes.length,
      thisAlloc: bucket.quota.ram,
      otherBuckets: totals.quotaUsedPerNode * bucket.nodes.length - bucket.quota.ram
    };
  }

  getDiskConfigParams(bucket) {
    if (!bucket) {
      return;
    }

    let totals = bucket.basicStats.storageTotals.hdd;
    return {
      total: totals.total,
      thisBucket: bucket.basicStats.diskUsed,
      otherBuckets: totals.usedByData - bucket.basicStats.diskUsed,
      otherData: totals.used - totals.usedByData
    };
  }

  getRamConfig(ramSummary) {
    if (!ramSummary) {
      return;
    }

    let config = {};
    config.topRight = {
      name: 'cluster quota',
      value: ramSummary.total
    };

    let available = ramSummary.total - ramSummary.otherBuckets - ramSummary.thisAlloc;
    config.items = [{
      name: 'other buckets',
      value: ramSummary.otherBuckets
    }, {
      name: 'this bucket',
      value: ramSummary.thisAlloc
    }, {
      name: 'available',
      value: available
    }];

    if (available < 0) {
      config.items[1].value = ramSummary.total - ramSummary.otherBuckets;
      config.items[2] = {
        name: 'overcommitted',
        value: ramSummary.otherBuckets + ramSummary.thisAlloc - ramSummary.total
      };
      config.topLeft = {
        name: 'total allocated',
        value: ramSummary.otherBuckets + ramSummary.thisAlloc
      };
    }

    return config;
  }

  getDiskConfig(diskSummary) {
    var config = {};

    let available = diskSummary.total - diskSummary.otherData - diskSummary.thisBucket - diskSummary.otherBuckets;
    config.topRight = {
      name: 'total cluster storage',
      value: diskSummary.total
    };
    config.items = [{
      name: 'other buckets',
      value: diskSummary.otherBuckets
    }, {
      name: 'this bucket',
      value: diskSummary.thisBucket
    }, {
      name: 'available',
      value: available
    }];

    return config;
  }

  getWarmUpTasks([tasks, bucket]) {
    return tasks.filter(task => {
      let isNeeded = task.bucket === bucket.name;
      if (isNeeded) {
        task.hostname = bucket.nodes.find(node => node.otpNode === task.node).hostname;
      }
      return isNeeded;
    });
  }

  prepareEjectionMethodText(bucket) {
    switch (bucket.bucketType) {
      case 'ephemeral':
        if (bucket.evictionPolicy === 'noEviction') {
          return 'No ejection';
        } else {
          return 'Eject data when RAM is full';
        }
      default:
        if (bucket.evictionPolicy === 'valueOnly') {
          return 'Value-Only';
        } else {
          return 'Full';
        }
    }
  }

  prepareStorageBackendText(bucket) {
    switch (bucket.storageBackend) {
      case 'couchstore':
        return 'CouchStore';
      case 'magma':
        return 'Magma';
      default:
        return '';
    }
  }

  prepareCompactionProgressText(compactionTask) {
    return compactionTask ? (compactionTask.progress + '% complete') : 'Not active';
  }

  getCompactionTask([compactionTasks, bucketName]) {
    return compactionTasks[bucketName] && compactionTasks[bucketName][0];
  }

  isCompactDisabled([startedCompactions, bucketControllers, compactionTask]) {
    return startedCompactions[bucketControllers.compactAll] || compactionTask;
  }

  isCancelCompactDisabled([startedCompactions, compactionTask]) {
    return compactionTask && startedCompactions[compactionTask.cancelURI];
  }

  showCompactBtn([compactionTask, bucketName, bucketType, permissions]) {
    return (!compactionTask || !compactionTask.cancelURI) &&
            bucketType === 'membase' &&
            permissions.cluster.tasks.read &&
            permissions.cluster.bucket[bucketName].compact;
  }

  showCancelCompactBtn([compactionTask, bucketName, permissions]) {
    return (compactionTask && compactionTask.cancelURI) &&
            permissions.cluster.tasks.read &&
            permissions.cluster.bucket[bucketName].compact;
  }

  showFlushBtn([controllers, bucketName, permissions]) {
    return controllers && controllers.flush &&
      permissions.cluster.bucket[bucketName].flush;
  }

  unpackData([autoCompactionSettings, reallyActiveKVNodes], storageTotals) {
    let ramQuota = 0;
    let ram = storageTotals.ram;
    if (ram) {
      ramQuota = Math.floor(
        (ram.quotaTotal - ram.quotaUsed) / reallyActiveKVNodes.length);
    }

    return {
      name: '',
      ramQuotaMB: this.mnHelperService.transformBytesToMB(ramQuota),
      bucketType: 'membase',
      replicaNumberEnabled: true,
      replicaNumber: 1,
      replicaIndex: 0,
      evictionPolicy: 'valueOnly',
      evictionPolicyEphemeral: 'noEviction',
      maxTTLEnabled: false,
      maxTTL: 0,
      compressionMode: 'passive',
      conflictResolutionType: 'seqno',
      flushEnabled: 0,
      threadsNumber: '3',
      purgeInterval: 3,
      durabilityMinLevel: 'none',
      storageBackend: 'couchstore',
      autoCompactionDefined: false,
      autoCompactionSettings
    };
  }

  getBucketFormData(defaultAutoCompaction, bucket) {
    let result = {
      name: bucket.name,
      ramQuotaMB: this.mnHelperService.transformBytesToMB(bucket.quota.rawRAM),
      bucketType: bucket.bucketType,
      replicaNumberEnabled: bucket.replicaNumber !== 0,
      replicaNumber: bucket.replicaNumber,
      replicaIndex: bucket.replicaIndex,
      evictionPolicy: bucket.evictionPolicy,
      evictionPolicyEphemeral: bucket.evictionPolicy,
      maxTTLEnabled: bucket.maxTTL !== 0,
      maxTTL: bucket.maxTTL,
      compressionMode: bucket.compressionMode,
      conflictResolutionType: bucket.conflictResolutionType,
      flushEnabled: (bucket.controllers && bucket.controllers.flush) ? 1 : 0,
      threadsNumber: bucket.threadsNumber + '',
      purgeInterval: bucket.purgeInterval,
      durabilityMinLevel: bucket.durabilityMinLevel,
      storageBackend: bucket.storageBackend,
      autoCompactionDefined: !!bucket.autoCompactionSettings,
    };

    let autoCompaction = this.mnSettingsAutoCompactionService.getSettingsSource(
      bucket.autoCompactionSettings ? bucket : {autoCompactionSettings: defaultAutoCompaction});
    autoCompaction.purgeInterval = bucket.autoCompactionSettings ?
      bucket.purgeInterval : defaultAutoCompaction.purgeInterval;
    result = Object.assign(result, {autoCompactionSettings: autoCompaction});

    return result;
  }

  createBucketFormData(bucket) {
    return this.stream.defaultAutoCompactionData
      .pipe(map(v => this.getBucketFormData(v, bucket)));
  }

  createInitialFormData(storageTotals) {
    return this.stream.defaultAutoCompactionData
      .pipe(withLatestFrom(this.mnAdminService.stream.reallyActiveKVNodes),
            map(v => this.unpackData(v, storageTotals)));
  }

  get(url) {
    return this.http.get(
      url,
      {params: new HttpParams().set('skipMap', true).set('basic_stats', true)}
    );
  }

  createPostBucketPipe(id) {
    this.stream.postBucket =
      new MnHttpRequest(this.postBucket.bind(this, false, id))
        .addSuccess()
        .addError();
    return this.stream.postBucket;
  }

  createPostValidationPipe(id) {
    this.stream.postBucketValidation =
      new MnHttpRequest(this.postBucket.bind(this, true, id))
        .addSuccess()
        .addError();
    return this.stream.postBucketValidation;
  }

  postBucket(justValidate, bucketId, payload) {
    let params = new HttpParams();
    if (justValidate) {
      params = params.set('just_validate', 1).set('ignore_warnings', 0);
    }

    let url = '/pools/default/buckets';
    if (bucketId) {
      url = url + '/' + payload.name;
      params = params.set('bucket_uuid', bucketId);
    }

    return this.http.post(url, payload, {params});
  }

  deleteBucket(bucket) {
    return this.http.delete(bucket.uri);
  }

  flushBucket(bucket) {
    return this.http.post(bucket.controllers.flush);
  }

  postCompact(postURL) {
    return this.http.post(postURL);
  }
}
