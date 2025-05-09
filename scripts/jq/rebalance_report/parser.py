# @author Couchbase <info@couchbase.com>
# @copyright 2023 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime


def get_vbucket_moves(report, bucket):
    if not report.get("is_rebalancing", True):
        print("Rebalance wasn't running")
        return [], None

    stage_infos = report.get("stageInfo")
    if stage_infos is None:
        print("No stageInfo found")
        return [], None

    data_info = stage_infos.get("data")
    if data_info is None:
        print("No data service rebalance found")
        return [], None

    details = data_info.get("details")
    if details is None:
        print("No details found")
        return [], None

    if bucket == "":
        if len(details) == 0:
            print("No bucket rebalance found")
            return [], None
        else:
            bucket = max(details.keys(),
                         key=lambda name: details[name]
                         ["startTime"])
    if bucket not in details:
        print(f"No details found for bucket '{bucket}'")
        return [], None

    bucket_details = details[bucket]
    vbs = bucket_details["vbucketLevelInfo"]["vbucketInfo"]

    start_time = min([timestamp(vb["move"]["startTime"])
                      for vb in vbs
                      if "startTime" in vb["move"] and
                      timestamp(vb["move"]["startTime"]) is not None])

    return [get_vbucket_move(vb, start_time) for vb in vbs
            if "startTime" in vb["move"] and
            vb["move"]["startTime"] is not False], bucket


def get_vbucket_move(vb, rebalance_start):
    start = timestamp(vb["move"]["startTime"]) - rebalance_start
    return {
            "vbucket": vb["id"],
            "type": move_type(vb),
            "start": start,
            "backfillDuration": backfill_duration(vb),
            "duration": duration(vb) if "completedTime" in vb["move"] else None
        }


def timestamp(string):
    if isinstance(string, str):
        if string[-1] == "Z":
            return datetime.datetime.fromisoformat(string[:-1]).timestamp()
        else:
            return datetime.datetime.fromisoformat(string).timestamp()
    else:
        return None


def move_type(vb):
    if vb["beforeChain"][0] != vb["afterChain"][0]:
        return "active"
    else:
        return "replica"


def backfill_duration(vb):
    if vb["move"]["startTime"] is False:
        return None
    elif "completedTime" not in vb["backfill"]:
        return None
    elif vb["backfill"]["completedTime"] is False:
        return None
    else:
        return (timestamp(vb["backfill"]["completedTime"]) -
                timestamp(vb["move"]["startTime"]))


def duration(vb):
    if vb["move"]["completedTime"] is False:
        return None
    elif vb["move"]["startTime"] is False:
        return None
    else:
        return (timestamp(vb["move"]["completedTime"]) -
                timestamp(vb["move"]["startTime"]))
