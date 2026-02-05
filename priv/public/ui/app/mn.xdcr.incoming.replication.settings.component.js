/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import {Component, ChangeDetectionStrategy} from '@angular/core';
import {MnLifeCycleHooksToStream} from "./mn.core.js";
import {NgbActiveModal} from '@ng-bootstrap/ng-bootstrap';

import {MnFormService} from './mn.form.service.js';
import template from "./mn.xdcr.incoming.replication.settings.html";
import {map} from "rxjs/operators";


export {MnXDCRIncomingReplicationSettingsComponent};

class MnXDCRIncomingReplicationSettingsComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      template,
      changeDetection: ChangeDetectionStrategy.OnPush,
      inputs: [
        "settings"
      ]
    })
  ]}

  static get parameters() { return [
    NgbActiveModal,
    MnFormService
  ]}

  constructor(activeModal, mnFormService) {
    super();

    this.activeModal = activeModal;
    this.form = mnFormService.create(this)
      .setFormGroup({})
      .hasNoPostRequest()
  }

  ngOnInit() {
    const mapPriority = priorityValue => {
      switch (priorityValue) {
        case 0:
          return "High";
        case 1:
          return "Medium";
        case 2:
          return "Low";
        default:
          return priorityValue;
      }
    };

    const mapMobile = mobileValue => {
      switch (mobileValue) {
        case 1:
          return "Off";
        case 2:
          return "Active";
        default:
          return mobileValue;
      }
    };

    const mapCompressionType = compressionValue => {
      switch (compressionValue) {
        case 3:
          return "Auto";
        case 1:
          return "None";
        default:
          return compressionValue;
      }
    };

    const mapLogLevel = logLevelValue => {
      switch (logLevelValue) {
        case 11:
          return "Error";
        case 12:
          return "Warn";
        case 13:
          return "Info";
        case 14:
          return "Debug";
        case 15:
          return "Trace";
        default:
          return logLevelValue;
      }
    };

    this.priority = this.settings.pipe(map((settings => mapPriority(settings.values.priority))));
    this.mobile = this.settings.pipe(map((settings => mapMobile(settings.values.mobile))));
    this.compressionType = this.settings.pipe(map((settings => mapCompressionType(settings.values.compression_type))));
    this.sourceNozzles = this.settings.pipe(map((settings => settings.values.source_nozzle_per_node)));
    this.targetNozzles = this.settings.pipe(map((settings => settings.values.target_nozzle_per_node)));
    this.checkpointInterval = this.settings.pipe(map((settings => settings.values.checkpoint_interval)));
    this.batchCount = this.settings.pipe(map((settings => settings.batch_count)));
    this.batchSize = this.settings.pipe(map((settings => settings.batch_size)));
    this.failureRetry = this.settings.pipe(map((settings => settings.failure_restart_interval)));
    this.optimisticThreshold = this.settings.pipe(map((settings => settings.optimistic_replication_threshold)));
    this.statsInterval = this.settings.pipe(map((settings => settings.stats_interval)));
    this.networkUsageLimit = this.settings.pipe(map((settings => settings.bandwidth_limit)));
    this.logLevel = this.settings.pipe(map((settings => mapLogLevel(settings.values.log_level))));
  }
}
