/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import * as pipes from './mn.pipes.js';
import {NgModule} from '@angular/core';
import {DecimalPipe} from '@angular/common';

export { MnPipesModule };

class MnPipesModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        pipes.MnParseVersion,
        pipes.MnMBtoBytes,
        pipes.MnBytesToMB,
        pipes.MnObjectKeys,
        pipes.MnPrettyVersion,
        pipes.MnFormatProgressMessage,
        pipes.MnFormatStorageModeError,
        pipes.MnPrepareQuantity,
        pipes.MnFormatUptime,
        pipes.MnFormatQuantity,
        pipes.MnFormatWarmupMessage,
        pipes.MnBucketsType,
        pipes.MnConflictResolutionType,
        pipes.MnTruncate,
        pipes.MnTruncateTo3Digits,
        pipes.MnFormatServices,
        pipes.MnOrderServices,
        pipes.MnStripPortHTML
      ],
      exports: [
        pipes.MnParseVersion,
        pipes.MnMBtoBytes,
        pipes.MnBytesToMB,
        pipes.MnObjectKeys,
        pipes.MnPrettyVersion,
        pipes.MnFormatProgressMessage,
        pipes.MnFormatStorageModeError,
        pipes.MnPrepareQuantity,
        pipes.MnFormatUptime,
        pipes.MnFormatQuantity,
        pipes.MnFormatWarmupMessage,
        pipes.MnBucketsType,
        pipes.MnConflictResolutionType,
        pipes.MnTruncate,
        pipes.MnTruncateTo3Digits,
        pipes.MnFormatServices,
        pipes.MnOrderServices,
        pipes.MnStripPortHTML
      ],
      imports: [],
      providers: [
        pipes.MnParseVersion,
        pipes.MnPrettyVersion,
        pipes.MnPrepareQuantity,
        pipes.MnBytesToMB,
        pipes.MnFormatQuantity,
        DecimalPipe,
        pipes.MnFormatServices,
        pipes.MnTruncateTo3Digits
      ]
    })
  ]}
}
