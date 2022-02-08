import { S as SCHEDULED, T as Transition } from './common/index-35caf4f7.js';
export { i as interrupt, t as transition } from './common/index-35caf4f7.js';
import './common/index-e88ffd88.js';
import './common/rgb-50db7803.js';
import './common/string-cfd0b55d.js';
import './common/index-f3df269c.js';

var root = [null];

function active(node, name) {
  var schedules = node.__transition,
      schedule,
      i;

  if (schedules) {
    name = name == null ? null : name + "";
    for (i in schedules) {
      if ((schedule = schedules[i]).state > SCHEDULED && schedule.name === name) {
        return new Transition([[node]], root, name, +i);
      }
    }
  }

  return null;
}

export { active };
