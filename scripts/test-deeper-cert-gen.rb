#!/usr/bin/ruby
#
# @author Couchbase <info@couchbase.com>
# @copyright 2014-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.

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
