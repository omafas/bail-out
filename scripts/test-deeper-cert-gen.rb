#!/usr/bin/ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2014 Couchbase, Inc.
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

GEN_PATH = File.join(File.dirname(__FILE__), "..", "deps", "generate_cert", "generate_cert.go")

def split_output(output)
  raise unless output =~ /-----BEGIN RSA/

  cert = $`
  pkey = output[cert.size..-1]
  raise if cert.empty? || pkey.empty?
  [cert, pkey]
end

def run!(suffix)
  cmdline = "go run #{GEN_PATH} #{suffix}".strip
  puts "# " + cmdline
  split_output(`#{cmdline}`)
end

ca_cert, ca_pkey = *run!("")

ENV['CACERT'] = ca_cert
ENV['CAPKEY'] = ca_pkey

cert, pkey = *run!("--generate-leaf --common-name=beta.local")
puts "ca_pkey:"
puts ca_pkey

puts "ca_cert:"
puts ca_cert

IO.popen("openssl x509 -noout -in /dev/stdin -text", "w") {|f| f << ca_cert}

puts "pkey:"
puts pkey

puts "cert:"
puts cert

IO.popen("openssl x509 -noout -in /dev/stdin -text", "w") {|f| f << cert}
