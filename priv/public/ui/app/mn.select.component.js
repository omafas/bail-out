/*
Copyright 2021-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

import { ChangeDetectionStrategy, Component } from '/ui/web_modules/@angular/core.js';
import { MnLifeCycleHooksToStream } from "./mn.core.js";
import { startWith } from '/ui/web_modules/rxjs/operators.js';
import { BehaviorSubject, Subject } from '/ui/web_modules/rxjs.js';
import { MnHelperService } from './mn.helper.service.js';
import { pluck, shareReplay, map, takeUntil, withLatestFrom } from '/ui/web_modules/rxjs/operators.js';

export { MnSelectComponent };

class MnSelectComponent extends MnLifeCycleHooksToStream {
  static get annotations() { return [
    new Component({
      selector: "mn-select",
      templateUrl: "app/mn.select.html",
      inputs: [
        "group",
        "mnFormControlName",
        "values",
        "labels",
        "valuesMapping",
        "capitalize",
        "mnPlaceholder",
        "placement",
        "hasSearch"
      ],
      changeDetection: ChangeDetectionStrategy.OnPush
    })
  ]}

  static get parameters() { return [
    MnHelperService
  ]}

  constructor(MnHelperService) {
    super();

    this.mnHelperService = MnHelperService;
  }

  ngOnInit() {
    this.placement = this.placement || 'bottom';

    if (!this.valuesMapping) {
      this.valuesMapping = this.defaultValuesMapping.bind(this);
    }

    if (this.hasSearch) {
      this.prepareSearch();
    }

    this.dropdownFormControl = this.group.get(this.mnFormControlName);
    if (this.dropdownFormControl) {
      this.disabled = new BehaviorSubject(this.dropdownFormControl.disabled);
      this.value = this.dropdownFormControl.valueChanges.pipe(startWith(this.dropdownFormControl.value));
      this.dropdownFormControl.registerOnDisabledChange(disabled => this.disabled.next(disabled));
    }
  }

  prepareSearch() {
    let searchMinimumOptionsNumber = 10;
    this.selectOptionClickStream = new Subject();
    this.selectLabelClickStream = new Subject();
    this.searchFilter = this.mnHelperService.createFilter(this);

    var valuesStream = this.mnOnChanges
      .pipe(pluck("values", "currentValue"));
    this.preparedValues = valuesStream
      .pipe(this.searchFilter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));

    this.hasSearchInput = valuesStream
      .pipe(map(values => (this.hasSearch && (values || []).length >= searchMinimumOptionsNumber) || false));

    this.selectOptionClickStream
      .pipe(takeUntil(this.mnOnDestroy))
      .subscribe(selectedOption => this.optionSelected(selectedOption));

    var labelsStream = this.mnOnChanges
      .pipe(pluck("labels", "currentValue"));
    this.preparedLabels = labelsStream
      .pipe(this.searchFilter.pipe,
            shareReplay({refCount: true, bufferSize: 1}));

    this.selectLabelClickStream
      .pipe(withLatestFrom(labelsStream, valuesStream),
            takeUntil(this.mnOnDestroy))
      .subscribe(([selectedLabel, labels, values]) => {
        this.optionSelected(values[labels.indexOf(selectedLabel)]);
      });
  }

  /**
   * Default values mapping:
   * * if capitalize input flag is true - capitalize the displayed label if it is a string
   * * else leave the label as it is
   * @param option
   * @returns {string}
   */
  defaultValuesMapping(option) {
    if (this.capitalize && angular.isString(option) && option) {
      return option[0].toUpperCase() + option.slice(1);
    }

    return option;
  }

  optionSelected(value) {
    this.dropdownFormControl.setValue(value);
    if (this.hasSearchInput) {
      this.searchFilter.group.get('value').setValue('');
    }
  }
}
