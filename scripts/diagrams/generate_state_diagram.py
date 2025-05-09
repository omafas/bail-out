#!/user/bin/env python3
# @author Couchbase <info@couchbase.com>
# @copyright 2023-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

import argparse
import re

"""
Utility to generate a mermaid state diagram (technically a flow chart due to
style limitations of the mermaid tool) from comments in the code of the form:

%% @state_change
%% @from foo
%% @to bar
%% @reason why

"""
pattern = r'%% @state_change\s*%% @from (\w*)\s*%% @to (\w*)\s*%% ' \
          r'@reason (.*)\n'

mermaid_start = '```mermaid\n'
style = '%%{ init: { \'flowchart\': { \'curve\': \'monotoneX\' } } }%%\n'
flowchart_LR  = 'flowchart LR\n'
mermaid_end   = '```'

def get_state_transitions(file_name: str, out_file:str):
    regex = re.compile(pattern, re.MULTILINE)

    with open(file_name, 'r') as file:
        matches = regex.findall(file.read())
        if matches:
            with open(out_file, 'w') as out:
                out.write(mermaid_start)
                out.write(style)
                out.write(flowchart_LR)
                for from_state, to_state, reason in matches:
                    out.write(f'{from_state} --> | {reason} | {to_state}\n')
                out.write(mermaid_end)


if __name__ == "__main__":
    argParser = argparse.ArgumentParser()
    argParser.add_argument('--input-file', '-i',
                           type=str,
                           help='File to parse')
    argParser.add_argument('--output-file', '-o',
                           type=str,
                           help='File to output diagram to')
    args = argParser.parse_args()
    get_state_transitions(args.input_file, args.output_file)
