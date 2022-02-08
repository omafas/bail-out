/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { Component, ChangeDetectionStrategy, ViewChild } from '@angular/core';
import { MnLifeCycleHooksToStream } from './mn.core.js';
import { MnHelperService } from './mn.helper.service.js';
import '../libs/codemirror.javascript.js';

export { MnCodeMirrorEditorComponent };

class MnCodeMirrorEditorComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-codemirror-editor",
      templateUrl: "app/mn.codemirror.editor.html",
      changeDetection: ChangeDetectionStrategy.OnPush,
      queries: {
        editor: new ViewChild('editor')
      },
      inputs: ['mnText', 'mnReadOnly', 'mnControl']
    })
  ]}

  static get parameters() { return [
    MnHelperService
  ]}

  constructor(mnHelperService) {
    super();

    this.mnHelperService = mnHelperService;
  }

  /* Instantiate an instance of codeMirror, default options passed in as a second argument.
     mnText input is set as the default text value.  */
  ngOnInit() {
    this.codeMirror = this.mnHelperService.createCodeMirror(this.editor.nativeElement, {
      lineNumbers: true,
      lineWrapping: true,
      matchBrackets: true,
      tabSize: 2,
      mode: { name: "javascript", json: true },
      readOnly: this.mnReadOnly || false
    });

    this.codeMirror.instance.setValue(this.mnText || "");

    if (this.mnControl) {
      this.codeMirror.onChange.subscribe(val => {
        let text = this.getValue(val);
        this.mnControl.patchValue(text);
      });
    }
  }

  /* Useful for setting the input if it changes from the parent component. */
  ngOnChanges() {
    if (this.codeMirror) {
      this.codeMirror.instance.setValue(this.mnText);
    }
  }

  getValue(event) {
    return event[0].getValue();
  }
}

