/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { NgModule } from '@angular/core';
import { MnCodeMirrorEditorComponent } from './mn.codemirror.editor.component.js';

export { MnCodeMirrorEditorModule };

class MnCodeMirrorEditorModule {
  static get annotations() { return [
    new NgModule({
      declarations: [
        MnCodeMirrorEditorComponent,
      ],
      exports: [
        MnCodeMirrorEditorComponent
      ]
    })
  ]}
}
