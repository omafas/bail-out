#!/usr/bin/env python3
#
# @author Couchbase <info@couchbase.com>
# @copyright 2025-Present Couchbase, Inc.
#
# Use of this software is governed by the Business Source License included in
# the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
# file, in accordance with the Business Source License, use of this software
# will be governed by the Apache License, Version 2.0, included in the file
# licenses/APL2.txt.
"""Script to analyze stats.log for memory, CPU usage, and pressure metrics.

Usage:
    python3 scripts/parse_stats_log.py -l <path_to_stats.log> [--nodes node1,
    node2] [-s <start_time>] [-e <end_time>] [-o <output_dir>]

Example:
    # Analyze specific nodes
    python3 scripts/parse_stats_log.py -l ns_server.stats.log --nodes
    "ns_1@node1,ns_1@node2" -o analysis

    # Analyze all nodes in the cluster
    python3 scripts/parse_stats_log.py -l ns_server.stats.log -s
    "2024-01-02T10:00" -e "2024-01-02T11:00" -o analysis

    # To compare per-node graphs across all analyzed nodes:
    open analysis/*/memory/erlang/top_memory_consumers.png

Output Structure:
analysis/                                  # Base output dir (-o to configure)
├── cluster/                               # Multi-node comparative analysis
│   ├── memory/                            # Memory-related stats
│   │   ├── allocstalls.png                # Memory allocation stalls over time
│   │   ├── top_erlang_consumer.png        # Highest Erlang memory consumer PID
│   │   ├── memory_pressure_avg.png        # % time stalled on mem (full_avg60)
│   │   └── memory_pressure_rate.png       # Rate of change in stalled time
│   ├── cpu/                               # CPU-related stats
│   │   ├── cpu_pressure_avg.png           # % time stalled on CPU (some_avg60)
│   │   └── cpu_pressure_rate.png          # Rate of change in stalled time
│   └── io/                                # IO-related stats
│       ├── io_pressure_avg.png            # % time stalled on IO (full_avg60)
│       └── io_pressure_rate.png           # Rate of change in stalled time
└── node_name/                             # One dir per analyzed node
    ├── summary.txt                        # Node statistics summary
    ├── memory/                            # Memory-related stats
    │   ├── processes/                     # Process-specific memory stats
    │   │   ├── process_memory.png         # Resident & virtual memory plots
    │   │   └── process_major_faults.png   # Page faults over time
    │   ├── erlang/                        # Erlang VM memory stats
    │   │   ├── memory_breakdown.png       # From erlang:memory
    │   │   └── top_memory_consumers.png   # From memsup:get_memory_data
    │   ├── os_memory.png                  # From memsup:get_system_memory_data
    │   ├── allocstalls.png                # Memory allocation stalls over time
    │   ├── memory_pressure_avg.png        # % time stalled on mem (full_avg60)
    │   └── memory_pressure_rate.png       # Rate of change in stalled time
    ├── cpu/                               # CPU-related stats
    │   ├── processes/                     # Process-specific CPU stats
    │   │   └── cpu_utilization.png        # Per-process CPU usage
    │   ├── os_cpu.png                     # OS-level CPU stats
    │   ├── cpu_pressure_avg.png           # % time stalled on CPU (some_avg60)
    │   └── cpu_pressure_rate.png          # Rate of change in stalled time
    └── io/                                # IO-related stats
        ├── io_pressure_avg.png            # % time stalled on IO (full_avg60)
        └── io_pressure_rate.png           # Rate of change in stalled time

Limitations:
- ns_doctor log timestamps don't indicate exact collection time for each node
- Data granularity is approximately 1 minute but intervals may vary due to:
  * System load affecting collection time
  * Network delays in stats propagation between nodes
- Collection gaps are detected using raw timestamps:
  * Gaps > 2 minutes between data points are reported in the summary
  * Stale data (same last_heard timestamp) is omitted, showing as gaps
  * These gaps indicate real collection or node responsiveness issues
- For visualization, this script:
  * Normalizes timestamps to the nearest minute for consistent plotting
  * A data point at 10:33:45 will be shown at 10:34:00
  * Missing data appears as gaps in line plots
  * Stale data (filtered out) appears as gaps
- For more accurate results, analyze each node's stats from its own
  ns_server.stats.log

Use cases:
1. Analyzing memory pressure situations where Prometheus stats are not logged
2. Analyzing memory consumption in Erlang (not reported to Prometheus)
3. Querying pressure metrics, which were not reported to Prometheus until 7.2.4
4. Comparing resource usage patterns across nodes in a cluster

Data Sources:
- Process stats: CPU and memory usage for each Couchbase Server process
- Erlang VM stats: Memory usage breakdown from the Erlang runtime
- OS stats: System-wide CPU and memory metrics
- Pressure stats: PSI (Pressure Stall Information) metrics from Linux kernel
"""
from datetime import datetime
from collections import defaultdict
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import argparse
import os


class StatsAnalyzer:
    STORAGE_UNITS = ["B", "KiB", "MiB", "GiB", "TiB"]

    def _parse_timestamp(self, ts_str, ts_type="timestamp"):
        """Parse timestamp string in YYYY-MM-DDThh:mm format.

        Args:
            ts_str: timestamp string to parse
            ts_type: type of timestamp for error message ("start time" or "end
             time")

        Returns:
            datetime object truncated to minute granularity

        Raises:
            ValueError if format is invalid
        """
        try:
            return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M")
        except ValueError:
            raise ValueError(f"Invalid {ts_type} format. Expected:"
                              "YYYY-MM-DDThh:mm")

    def __init__(self, log_file, ts_start=None, ts_end=None):
        """Initialize analyzer with optional time range ts_start, ts_end:

        datetime strings in format "YYYY-MM-DDThh:mm".
        """
        self.ts_start = None
        self.ts_end = None

        if ts_start and ts_end:
            self.ts_start = self._parse_timestamp(ts_start, "start time")
            self.ts_end = self._parse_timestamp(ts_end, "end time")
            if self.ts_start >= self.ts_end:
                raise ValueError("Start time must be before end time")
        elif ts_start:
            self.ts_start = self._parse_timestamp(ts_start, "start time")
        elif ts_end:
            self.ts_end = self._parse_timestamp(ts_end, "end time")

        try:
            self.df = self._parse_stats_log(log_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Log file not found: {log_file}")
        except Exception as e:
            raise ValueError(f"Failed to parse log file: {str(e)}")

        if len(self.df) == 0:
            raise ValueError("No data found in log file")

        data_start = self.df["timestamp"].min()
        data_end = self.df["timestamp"].max()

        # The log may have wrapped. Check for overlap of available timestamps
        # with the specified timestamp range.
        if self.ts_start and self.ts_start > data_end:
            raise ValueError(
                f"Start time {self.ts_start} is after last available data "
                f"point {data_end}. Available time range: {data_start} to "
                f"{data_end}"
            )
        if self.ts_end and self.ts_end < data_start:
            raise ValueError(
                f"End time {self.ts_end} is before first available data point "
                f"{data_start}. Available time range: {data_start} to "
                f"{data_end}"
            )

        if self.ts_start:
            self.df = self.df[self.df["timestamp"] >= self.ts_start]
        if self.ts_end:
            self.df = self.df[self.df["timestamp"] <= self.ts_end]

        self.df["time"] = self.df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")

        self.nodes = []
        self.specified_nodes = []

    def _parse_stats_log(self, log_file):
        """Parse ns_doctor log file and return DataFrame with stats.

        Raises exception if any parsing error.
        """
        entries = []
        heard = defaultdict(str)
        current_entries = {}  # Track entries by timestamp and node

        try:
            with open(log_file, "r") as f:
                lines = f.readlines()
        except (IOError, OSError) as e:
            raise IOError(f"Failed to read log file {log_file}: {str(e)}")

        current_entry = None
        mode = None
        current_process = None
        current_timestamp = None

        try:
            for i, line in enumerate(lines):
                # Parse timestamp
                if "ns_doctor:debug," in line:
                    timestamp_match = re.search(
                        r"ns_doctor:debug,("
                        r"[0-9]+-[0-9]+-[0-9]+T[0-9]+:[0-9]+:[0-9]+)",
                        line,
                    )
                    if not timestamp_match:
                        raise ValueError("Invalid timestamp format at line "
                                         f"{i}")
                    current_timestamp = datetime.strptime(
                        timestamp_match.group(1), "%Y-%m-%dT%H:%M:%S"
                    )
                    current_entries.clear()

                # Parse node (ns_1@node1, ns_1@node2, n_1@192.168.1.1...)
                elif re.match(r"[\[ ]{'(?:ns_1@|n_\d+@)", line):
                    if not current_timestamp:
                        raise ValueError("Node entry without timestamp at "
                                         f"line {i}")

                    node_match = re.search(r"{'([^']+)'", line)
                    if not node_match:
                        raise ValueError(f"Invalid node format at line {i}")

                    if current_entry:
                        entries.append(current_entry)

                    node = node_match.group(1)
                    assert (
                        current_timestamp,
                        node,
                    ) not in current_entries, (
                        f"Duplicate entry found for node {node} at timestamp "
                        f"{current_timestamp}"
                    )

                    current_entry = {
                        "node": node,
                        "timestamp": current_timestamp,
                    }
                    current_entries[(current_timestamp, node)] = current_entry

                # Check if stats are stale
                elif "last_heard" in line and current_entry:
                    last_heard_match = re.search(r"{last_heard,([-]*[0-9]+)",
                                                 line)
                    last_heard = last_heard_match.group(1)
                    if heard[current_entry["node"]] == last_heard:
                        # Discard stale entry. See merge comment for more info.
                        current_entry = None
                    else:
                        heard[current_entry["node"]] = last_heard

                # Parse system stats
                elif current_entry and "{system_stats," in line:
                    assert (
                        mode is None
                    ), f"Expected mode None, but was {mode} when parsing "
                    f"system_stats"
                    mode = "system_stats"
                elif mode == "system_stats":
                    stat_match = re.search(r"\{([^,]+),([0-9.]+)\}", line)
                    if stat_match:
                        stat_name = stat_match.group(1)
                        # Keep allocstall and all CPU stats
                        if (
                            stat_name == "allocstall"
                            or "cpu" in stat_name.lower()
                            or "cores" in stat_name.lower()
                        ):
                            current_entry[f"system_{stat_name}"] = float(
                                stat_match.group(2)
                            )
                    if "]}" in line:
                        mode = None

                # Parse memory data (memsup:get_memory_data())
                elif current_entry and "{memory_data," in line:
                    assert mode is None, (
                        f"Expected mode None, but was {mode} when parsing "
                        "memory_data"
                    )
                    mem_match = re.search(
                        r"\{memory_data,\{(\d+),(\d+),\{([^,]+),(\d+)\}\}\}",
                        line,
                    )
                    if mem_match:
                        # Only keep worst pid memory usage
                        current_entry["memory_worst_pid"] = mem_match.group(3)
                        current_entry["memory_worst_used"] = int(
                            mem_match.group(4))

                elif current_entry and "{disk_data," in line:
                    assert mode is None, (
                        f"Expected mode None or disk_data, but was {mode} when "
                        "parsing disk_data"
                    )
                    mode = "disk_data"
                elif mode == "disk_data":
                    # We're looking for this format for disk_data, we'll match
                    # a group for the name, the capacity, and the usage
                    #   [{"/dev",4096,0},
                    #    {"/dev/shm",524288,1},
                    #    {"/run",52655624,1},
                    #    {"/sys/fs/cgroup",4096,0},
                    #    {"/",20466256,27},
                    #    {"/export",853726716,5},
                    #    {"/tmp",524288,1},
                    #    {"/var",16337788,18}]}]},
                    disk_match = re.search(r"(\"\/[a-z]*\"),([0-9]*),([0-9]*)",
                                           line)
                    if disk_match:
                        disk = disk_match.group(1)
                        capacity = int(disk_match.group(2))
                        usage = int(disk_match.group(3))

                        current_entry[f"disk_{disk}_usage"] = usage
                        current_entry[f"disk_{disk}_capacity"] = capacity

                    if "}]}]}" in line:
                        mode = None

                # Parse system memory data (memsup:get_system_memory_data())
                elif current_entry and "{system_memory_data," in line:
                    assert (
                        mode is None
                    ), f"Expected mode None, but was {mode} when parsing "
                    f"system_memory_data"
                    mode = "system_memory_data"
                elif mode == "system_memory_data":
                    # Format: [{available_memory,N}, {buffered_memory,N},
                    #  {cached_memory,N}, {free_memory,N}, {free_swap,N},
                    #  {system_total_memory,N}, {total_memory,N},
                    #  {total_swap,N}]
                    mem_match = re.search(r"\{([^,]+),(\d+)\}", line)
                    if mem_match:
                        key = mem_match.group(1)
                        value = int(mem_match.group(2))
                        current_entry[f"system_{key}"] = value
                    if "]}" in line:
                        mode = None

                # Parse Erlang memory breakdown
                elif current_entry and "{memory," in line:
                    assert (
                        mode is None
                    ), f"Expected mode None, but was {mode} when parsing memory"
                    mode = "memory"
                elif mode == "memory":
                    # Format: [{total,Size}, {processes,Size}, {processes_used,
                    # Size},
                    # ...]
                    mem_match = re.search(r"\{([^,]+),(\d+)\}", line)
                    if mem_match:
                        category = mem_match.group(1)
                        value = int(mem_match.group(2))
                        current_entry[f"memory_{category}"] = value
                    if "]}" in line:
                        mode = None

                # Parse process stats
                elif (
                    current_entry
                    and "{processes_stats," in line
                    and "{processes_stats,[]}," not in line
                ):
                    assert (
                        mode is None
                    ), f"Expected mode None, but was {mode} when parsing "
                    "processes_stats"
                    # Peek at next line to determine format
                    next_line = lines[i + 1].strip()
                    if "<<" in next_line:
                        mode = "processes_stats_before_7_2_4"
                    else:
                        mode = "processes_stats_7_2_4_onwards"

                elif mode == "processes_stats_before_7_2_4":
                    # Format: {<<"process/stat">>, value}
                    binary_match = re.search(
                        r"{<<\"([\S]+)/([\S]+)\">>,(\d*\.*\d+)}", line
                    )
                    if binary_match:
                        process = binary_match.group(1)
                        stat = binary_match.group(2)
                        value = binary_match.group(3)
                        try:
                            value = int(value)
                        except ValueError:
                            value = float(value)
                        key = f"{process}_{stat}"
                        current_entry[key] = value

                    if "}]}," in line:
                        mode = None

                elif mode == "processes_stats_7_2_4_onwards":
                    # Format: {process_name, [{stat, value}, ...]}
                    process_match = re.search(r"\{([^,]+),$", line)
                    if process_match:
                        current_process = process_match.group(1).strip("'")

                    stat_match = re.search(r"\{([^,]+),([0-9.]+)\}", line)
                    if stat_match and current_process:
                        stat_name = stat_match.group(1).strip("'")
                        value = float(stat_match.group(2))
                        key = f"{current_process}_{stat_name}"
                        current_entry[key] = value

                    if "}]}]}" in line:
                        mode = None

                # Parse pressure stats
                elif current_entry and "_pressure" in line:
                    try:
                        pressure_match = re.search(r"\{([\w]+)_pressure", line)
                        if pressure_match:
                            # cpu, memory, or io
                            pressure_type = pressure_match.group(1)

                            # Parse "some" pressure stats
                            some_match = re.search(
                                r'<<"some avg10=([0-9]+.[0-9]+) '
                                "avg60=([0-9]+.[0-9]+) "
                                "avg300=([0-9]+.[0-9]+) "
                                "total=([0-9]+)",
                                lines[i + 1],
                            )
                            if some_match:
                                prefix = f"system_{pressure_type}_pressure_some"
                                current_entry[f"{prefix}_avg60"] = float(
                                    some_match.group(2)
                                )
                                current_entry[f"{prefix}_total"] = int(
                                    some_match.group(4)
                                )

                            # Parse "full" pressure stats (not present for CPU)
                            full_match = re.search(
                                r"full avg10=([0-9]+.[0-9]+) "
                                "avg60=([0-9]+.[0-9]+) "
                                "avg300=([0-9]+.[0-9]+) "
                                "total=([0-9]+)",
                                lines[i + 1],
                            )
                            if full_match:
                                prefix = f"system_{pressure_type}_pressure_full"
                                current_entry[f"{prefix}_avg60"] = float(
                                    full_match.group(2)
                                )
                                current_entry[f"{prefix}_total"] = int(
                                    full_match.group(4)
                                )
                    except Exception as e:
                        raise ValueError(
                            f"Failed to parse pressure stats at line {i+1}: "
                            f"{str(e)}"
                        )

        except Exception as e:
            raise ValueError(f"Failed to parse log file at line {i}: {str(e)}")

        if current_entry:
            entries.append(current_entry)

        # Convert to initial DataFrame (this may have gaps)
        df = pd.DataFrame(entries)
        if len(df) == 0:
            if os.path.basename(log_file) == "stats.log":
                details = " (perhaps try 'ns_server.stats.log')"
            else:
                details = ""
            raise ValueError(f"No valid entries found in {log_file} {details}")

        # Detect raw collection gaps using original timestamps
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        raw_gaps = []
        for node in df["node"].unique():
            node_data = df[df["node"] == node].sort_values("timestamp")
            time_diffs = node_data["timestamp"].diff()

            gap_starts = node_data[time_diffs >= pd.Timedelta(minutes=2)].index
            for idx in gap_starts:
                row_num = node_data.index.get_loc(idx)
                if row_num > 0:
                    gap_start = node_data.iloc[row_num-1]["timestamp"]
                    gap_end = node_data.iloc[row_num]["timestamp"]
                    raw_gaps.append({
                        "node": node,
                        "start": gap_start,
                        "end": gap_end,
                        "duration_mins": (
                            gap_end - gap_start
                        ).total_seconds() / 60
                    })

        # Keep raw DataFrame for summary statistics
        self.raw_df = df.copy()

        # Create normalized DataFrame for visualization
        df["timestamp"] = df["timestamp"].dt.round("1min")
        nodes = df["node"].unique()
        min_time = df["timestamp"].min()
        max_time = df["timestamp"].max()

        # Create complete minute-by-minute index for even spacing
        full_range = pd.date_range(start=min_time, end=max_time, freq="1min")

        # Create empty DataFrame with all combinations of timestamps and nodes
        index = pd.MultiIndex.from_product(
            [full_range, nodes], names=["timestamp", "node"]
        )
        full_df = pd.DataFrame(index=index).reset_index()

        # Merge to create evenly spaced timeline with NaN for missing data.
        # NaN values in the data (gaps in collection) are automatically handled
        # by matplotlib, which breaks the line at NaN values, creating visible
        # gaps in the plot. These gaps can indicate periods where:
        # - Data collection was interrupted
        # - The node was unresponsive
        # - The stats were stale (detected by last_heard)
        df = pd.merge(full_df, df, on=["timestamp", "node"], how="left")

        df.attrs["raw_collection_gaps"] = raw_gaps

        df["time"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M")
        df = df.sort_values(["timestamp", "node"])

        return df

    def generate_analysis(self, output_dir="analysis"):
        """Generate analysis output."""
        output_dir = Path(output_dir)
        output_dir.mkdir(exist_ok=True)

        # Determine cardinality: -1 for all nodes, actual count otherwise
        all_cluster_nodes = set(self.df["node"].unique())
        if set(self.nodes) == all_cluster_nodes:
            node_count = -1
        else:
            node_count = len(self.nodes)

        if node_count != 1:
            print("\nGenerating multi-node analysis...")
            cluster_dir = output_dir / "cluster"
            cluster_dir.mkdir(exist_ok=True)

            cluster_cpu_dir = cluster_dir / "cpu"
            cluster_memory_dir = cluster_dir / "memory"
            cluster_io_dir = cluster_dir / "io"
            cluster_cpu_dir.mkdir(exist_ok=True)
            cluster_memory_dir.mkdir(exist_ok=True)

            cluster_data = self.df[self.df["node"].isin(self.nodes)]
            self._plot_cluster_top_erlang_process(cluster_data,
                                                  cluster_memory_dir,
                                                  node_count)
            self._plot_cluster_system_stats(cluster_data, cluster_dir,
                                            node_count)
            self._plot_pressure_stats(cluster_data, cluster_dir,
                                      node_count)
            self._plot_cluster_allocstalls(cluster_data, cluster_dir,
                                           node_count)

        # Generate per-node analysis
        for node in self.nodes:
            print(f"\nAnalyzing {node}...")
            node_dir = output_dir / node
            node_dir.mkdir(exist_ok=True)

            # Create directory structure
            memory_dir = node_dir / "memory"
            memory_dir.mkdir(exist_ok=True)

            memory_processes_dir = memory_dir / "processes"
            memory_processes_dir.mkdir(exist_ok=True)

            memory_erlang_dir = memory_dir / "erlang"
            memory_erlang_dir.mkdir(exist_ok=True)

            cpu_dir = node_dir / "cpu"
            cpu_dir.mkdir(exist_ok=True)

            cpu_processes_dir = cpu_dir / "processes"
            cpu_processes_dir.mkdir(exist_ok=True)

            io_dir = node_dir / "io"
            io_dir.mkdir(exist_ok=True)

            node_data = self.df[self.df["node"] == node]

            print(f"Data points: {len(node_data)}")
            self._write_summary(node_data, node_dir / "summary.txt")

            # Generate node-specific plots
            self._plot_process_memory(node_data, memory_processes_dir)
            self._plot_memory_breakdown(node_data, memory_erlang_dir)
            self._plot_top_memory_consumers(node_data, memory_erlang_dir)
            self._plot_cpu_utilization(node_data, cpu_processes_dir)
            self._plot_os_memory_stats(node_data, memory_dir)
            self._plot_os_cpu_stats(node_data, cpu_dir)
            self._plot_pressure_stats(node_data, node_dir, 1)
            self._plot_disk_stats(node_data, io_dir)

    def _format_bytes(self, bytes_value):
        """Format bytes to appropriate unit."""
        value = float(bytes_value)

        unit_index = 0
        while value >= 1024 and unit_index < len(self.STORAGE_UNITS) - 1:
            value /= 1024
            unit_index += 1

        return value, self.STORAGE_UNITS[unit_index]

    def _convert_to_bytes(self, value_str):
        """Convert memory string with units to bytes."""
        value, unit = value_str.split()
        value = float(value)

        return value * 1024 ** self.STORAGE_UNITS.index(unit)

    def _plot_process_memory(self, data, output_dir):
        """Plot both resident and virtual memory size for processes."""
        processes = [
            col.replace("_mem_resident", "")
            for col in data.columns
            if col.endswith("_mem_resident")
        ]

        if not processes:
            print("Warning: No process memory data found to plot")
            return

        # Create figure with two subplots
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 16))

        # Plot resident memory
        max_resident = max(
            data[f"{process}_mem_resident"].max() for process in processes
        )
        _, unit_resident = self._format_bytes(max_resident)
        divisor_resident = 1024 ** self.STORAGE_UNITS.index(unit_resident)

        for i, process in enumerate(processes):
            resident_col = f"{process}_mem_resident"
            size_col = f"{process}_mem_size"

            style = self._get_process_style(i)

            # Plot resident memory
            converted_data = data[resident_col] / divisor_resident
            ax1.plot(data["timestamp"], converted_data, label=process, **style)

            # Plot virtual memory size
            converted_data = data[size_col] / divisor_resident
            ax2.plot(data["timestamp"], converted_data, **style)

        node_name = data["node"].iloc[0]
        ax1.set_title(f"Process Resident Memory - {node_name}")
        ax1.set_ylabel(f"Memory ({unit_resident})")
        self._adjust_x_axis(ax1)
        ax1.legend(bbox_to_anchor=(1.05, 1))
        ax2.set_title(f"Process Virtual Memory Size - {node_name}")
        ax2.set_ylabel(f"Memory ({unit_resident})")
        self._adjust_x_axis(ax2)

        plt.tight_layout()
        plt.savefig(output_dir / "process_memory.png")
        plt.close()

        plt.figure(figsize=(15, 8))
        for i, process in enumerate(processes):
            fault_col = f"{process}_major_faults_raw"
            if fault_col in data.columns:
                style = self._get_process_style(i)
                plt.plot(data["timestamp"], data[fault_col], label=process,
                         **style)

        plt.title("Process Major Page Faults")
        plt.ylabel("Major Faults")

        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.savefig(output_dir / "process_major_faults.png")
        plt.close()

    def _plot_memory_breakdown(self, data, output_dir):
        """Plot memory breakdown showing different memory categories over
        time."""
        memory_categories = [
            "memory_processes",
            "memory_processes_used",
            "memory_system",
            "memory_atom",
            "memory_atom_used",
            "memory_binary",
            "memory_code",
            "memory_ets",
        ]

        available_categories = [
            cat for cat in memory_categories if cat in data.columns
        ]
        if not available_categories:
            print("Warning: No memory categories found to plot")
            return

        plt.figure(figsize=(15, 8))

        # Find appropriate unit based on max value across all categories
        max_bytes = max(data[cat].max() for cat in available_categories)
        _, unit = self._format_bytes(max_bytes)
        divisor = 1024 ** self.STORAGE_UNITS.index(unit)

        for category in available_categories:
            converted_data = data[category] / divisor
            plt.plot(
                data["timestamp"],
                converted_data,
                label=category.replace("memory_", ""),
            )

        node_name = data["node"].iloc[0]
        plt.title(f"Memory Breakdown Over Time (erlang:memory) - {node_name}")

        self._adjust_x_axis(plt.gca())
        plt.ylabel(f"Memory ({unit})")
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.savefig(output_dir / "memory_breakdown.png")
        plt.close()

    def _plot_os_memory_stats(self, data, output_dir):
        """Plot memory stats from OS (memsup:get_system_memory_data())"""
        # Group metrics by type based on memsup documentation
        memory_metrics = {
            "emulator": [
                "system_total_memory",  # Total memory available to Erlang VM
                "system_free_memory",  # Free memory available to Erlang VM
            ],
            "system": [
                "system_system_total_memory",  # Total OS memory
                "system_buffered_memory",  # Memory for disk blocks
                "system_cached_memory",  # Memory for file cache
            ],
            "swap": [
                "system_total_swap",  # Total swap available
                "system_free_swap",  # Free swap available
            ],
        }

        available_metrics = [
            m for group in memory_metrics.values()
            for m in group if m in data.columns
        ]
        if not available_metrics:
            print("Warning: No OS memory metrics found to plot")
            return

        plt.figure(figsize=(15, 8))

        # Find appropriate unit based on max value
        max_bytes = max(data[metric].max() for metric in available_metrics)
        _, unit = self._format_bytes(max_bytes)
        divisor = 1024 ** self.STORAGE_UNITS.index(unit)

        # Plot with different line styles for different types
        styles = {
            "emulator": "-",  # solid for emulator memory
            "system": "--",  # dashed for system memory
            "swap": ":",  # dotted for swap
        }

        for group, metrics in memory_metrics.items():
            for metric in metrics:
                if metric in data.columns:
                    converted_data = data[metric] / divisor
                    # Keep system_ prefix only for system_total_memory to
                    # distinguish it from total_memory
                    if metric == "system_system_total_memory":
                        label = "system_total_memory"
                    else:
                        label = metric.replace("system_", "")
                    plt.plot(
                        data["timestamp"],
                        converted_data,
                        label=label,
                        linestyle=styles[group],
                    )

        node_name = data["node"].iloc[0]
        plt.title(
            "OS Memory Statistics (memsup:get_system_memory_data) - "
            f"{node_name}"
        )
        plt.xlabel("Time")
        plt.ylabel(f"Memory ({unit})")

        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.savefig(output_dir / "os_memory.png")
        plt.close()

        if "system_allocstall" in data.columns:
            plt.figure(figsize=(15, 8))
            plt.plot(data["timestamp"], data["system_allocstall"], color="red")

            node_name = data["node"].iloc[0]
            plt.title(f"Memory Allocation Stalls - {node_name}")
            plt.xlabel("Time")
            plt.ylabel("Stalls")
            self._adjust_x_axis(plt.gca())

            plt.tight_layout()
            plt.savefig(output_dir / "allocstalls.png")
            plt.close()

    def _plot_os_cpu_stats(self, data, output_dir):
        """Plot CPU stats from OS."""
        metrics = ["system_cpu_utilization_rate", "system_cpu_stolen_rate"]
        available_metrics = [m for m in metrics if m in data.columns]
        if not available_metrics:
            print("Warning: No OS CPU metrics found to plot")
            return

        plt.figure(figsize=(15, 8))

        for i, metric in enumerate(metrics):
            if metric in data.columns:
                style = self._get_process_style(i)
                plt.plot(
                    data["timestamp"],
                    data[metric],
                    label=metric.replace("system_", "").replace("_rate", ""),
                    **style,
                )

        node_name = data["node"].iloc[0]
        plt.title(f"OS CPU Statistics - {node_name}")
        plt.xlabel("Time")
        plt.ylabel("Percentage (%)")

        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.savefig(output_dir / "os_cpu.png")
        plt.close()

    def set_nodes(self, nodes_str):
        """Set nodes to analyze. If none specified, analyze all nodes."""
        available_nodes = sorted(self.df["node"].unique())

        if nodes_str:
            specified_nodes = [n.strip() for n in nodes_str.split(",")]
            invalid_nodes = [
                n for n in specified_nodes if n not in available_nodes]
            if invalid_nodes:
                print("\nError: The following nodes were not found in the log:")
                for node in invalid_nodes:
                    print(f"  - {node}")
                print("\nAvailable nodes:")
                for node in available_nodes:
                    print(f"  - {node}")
                return False
            self.nodes = specified_nodes
        else:
            self.nodes = available_nodes

        return True

    def _plot_top_memory_consumers(self, data, output_dir):
        """Plot memory usage by top memory consuming processes at each point
        in time."""
        if "memory_worst_pid" not in data.columns:
            return

        plt.figure(figsize=(15, 8))

        max_bytes = data["memory_worst_used"].max()
        _, unit = self._format_bytes(max_bytes)
        divisor = 1024 ** self.STORAGE_UNITS.index(unit)

        pid_counts = data["memory_worst_pid"].dropna().value_counts()
        unique_pids = sorted(
            pid_counts.index.tolist(),
            key=lambda x: pid_counts[x],
            reverse=True
        )

        colors = plt.cm.tab20(np.linspace(0, 1, len(unique_pids)))

        for i, pid in enumerate(unique_pids):
            mask = data["memory_worst_pid"] == pid
            count = mask.sum()
            node = data.loc[mask, "node"].iloc[0]
            plt.scatter(
                data.loc[mask, "timestamp"],
                data.loc[mask, "memory_worst_used"] / divisor,
                label=f"PID {pid} ({count} times)",
                color=colors[i],
                alpha=0.6,
                s=50,
            )

        node_name = data["node"].iloc[0]
        plt.title(f"Top Memory Consumers (Processes) - {node_name}")
        plt.xlabel("Time")
        plt.ylabel(f"Memory Usage ({unit})")
        self._adjust_x_axis(plt.gca())

        plt.legend(
            title="PID (Frequency)",
            bbox_to_anchor=(1.05, 1)
        )
        plt.tight_layout()
        plt.savefig(output_dir / "top_memory_consumers.png")
        plt.close()

    def _adjust_x_axis(self, ax):
        """Adjust x-axis to show a reasonable number of labels based on data
        points."""
        ticks = ax.xaxis.get_ticklocs()
        num_ticks = len(ticks)

        target_labels = 12

        # Calculate step size to get close to target number of labels
        n = max(1, num_ticks // target_labels)

        # Ensure we don't show fewer than 6 labels
        if num_ticks / n < 6:
            n = max(1, num_ticks // 6)

        ax.xaxis.set_major_formatter(
            plt.matplotlib.dates.DateFormatter("%Y-%m-%d %H:%M")
        )

        ax.xaxis.set_ticks(ticks[::n])
        plt.setp(ax.xaxis.get_ticklabels(), rotation=45, ha="right")

        ax.grid(True, axis="x", linestyle=":", alpha=0.2)

    def _get_process_style(self, index):
        """Get distinct style for each process."""
        # Custom color palette with distinct colors
        colors = [
            "#e41a1c",
            "#377eb8",
            "#4daf4a",
            "#984ea3",
            "#ff7f00",
            "#a65628",
            "#f781bf",
            "#999999",
            "#66c2a5",
            "#fc8d62",
        ]

        # Different line styles
        styles = ["-", "--", "-.", ":"]

        # Cycle through combinations of colors and styles
        color = colors[index % len(colors)]
        style = styles[(index // len(colors)) % len(styles)]

        return {"color": color, "linestyle": style, "linewidth": 2}

    def _plot_cpu_utilization(self, data, output_dir):
        """Plot CPU utilization across processes, normalized by CPU count."""
        processes = [
            col.replace("_cpu_utilization", "")
            for col in data.columns
            if col.endswith("_cpu_utilization")
        ]
        if not processes:
            print("Warning: No CPU utilization data found to plot")
            return

        plt.figure(figsize=(15, 8))

        cpu_count = None
        if "system_cpu_cores_available" in data.columns:
            cpu_count = data["system_cpu_cores_available"].mode().iloc[0]
        else:
            print(
                "Warning: CPU count not found in data, using raw utilization "
                "values"
            )

        for i, process in enumerate(processes):
            column = f"{process}_cpu_utilization"
            if cpu_count:
                normalized_data = data[column] / cpu_count
                label = process
            else:
                normalized_data = data[column]
                label = process

            style = self._get_process_style(i)
            plt.plot(
                data["timestamp"],
                normalized_data,
                label=label.replace("_cpu_utilization", ""),
                **style,
            )

        node_name = data["node"].iloc[0]
        plt.title(
            f"CPU Utilization Across Processes - {node_name}"
            + (" (Normalized by CPU Count)" if cpu_count else "")
        )
        plt.ylabel(
            "CPU Utilization (% per core)" if cpu_count else "CPU Utilization "
            "(%)"
        )
        self._adjust_x_axis(plt.gca())

        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()
        plt.savefig(output_dir / "cpu_utilization.png")
        plt.close()

    def _format_stall_time(self, microseconds):
        """Convert microseconds to most appropriate unit."""
        if microseconds >= 60_000_000:
            return microseconds / 60_000_000, "minutes"
        elif microseconds >= 1_000_000:
            return microseconds / 1_000_000, "seconds"
        elif microseconds >= 1_000:
            return microseconds / 1_000, "ms"
        return microseconds, "µs"

    def _plot_pressure_stats(self, data, output_dir, node_count):
        """Plot pressure statistics for CPU, memory and IO."""
        pressure_metrics = {
            "cpu": {
                "avg": "system_cpu_pressure_some_avg60",
                "total": "system_cpu_pressure_some_total"
            },
            "memory": {
                "avg": "system_memory_pressure_full_avg60",
                "total": "system_memory_pressure_full_total"
            },
            "io": {
                "avg": "system_io_pressure_full_avg60",
                "total": "system_io_pressure_full_total"
            }
        }

        has_pressure_metrics = any(
            metrics["avg"] in data.columns
            for metrics in pressure_metrics.values()
        )

        if not has_pressure_metrics:
            return

        for resource, metrics in pressure_metrics.items():
            if metrics["avg"] not in data.columns:
                continue

            # Plot average pressure (percentage of time stalled)
            plt.figure(figsize=(15, 8))
            if node_count != 1:
                for node in self.nodes:
                    node_data = data[data["node"] == node]
                    style = self._get_process_style(len(plt.gca().lines))
                    plt.plot(
                        node_data["timestamp"],
                        node_data[metrics["avg"]],
                        label=node,
                        **style,
                    )
            else:
                plt.plot(
                    data["timestamp"],
                    data[metrics["avg"]],
                    color="blue",
                )

            pressure_type = "some_avg60" if resource == "cpu" else "full_avg60"
            title = f"{resource.upper()} Pressure ({pressure_type}) "
            if node_count == 1:
                title += f"- {data['node'].iloc[0]}"
            elif node_count == -1:
                title += "Across Cluster"
            else:
                title += f"Across {node_count} Nodes"
            plt.title(title)
            plt.xlabel("Time")
            plt.ylabel("Percentage of time stalled")
            if node_count != 1:
                plt.legend(bbox_to_anchor=(1.05, 1))
            self._adjust_x_axis(plt.gca())

            plt.tight_layout()

            resource_dir = output_dir / resource
            resource_dir.mkdir(exist_ok=True)
            plt.savefig(
                resource_dir / f"{resource}_pressure_avg.png",
                bbox_inches="tight",
            )
            plt.close()

            # Plot rate of change in stall time
            plt.figure(figsize=(15, 8))
            if node_count != 1:
                for node in self.nodes:
                    node_data = data[data["node"] == node]
                    if metrics["total"] in node_data.columns:
                        style = self._get_process_style(len(plt.gca().lines))
                        # Calculate rate of change (microseconds per minute)
                        stall_time = node_data[metrics["total"]]
                        rate = stall_time.diff() / 60  # per minute
                        _, unit = self._format_stall_time(rate.max())

                        plt.plot(
                            node_data["timestamp"],
                            rate / (
                                1_000_000 if unit == "seconds" else
                                60_000_000 if unit == "minutes" else
                                1_000 if unit == "ms" else 1
                            ),
                            label=f"{node}",
                            **style,
                        )
            else:
                stall_time = data[metrics["total"]]
                rate = stall_time.diff() / 60  # per minute
                _, unit = self._format_stall_time(rate.max())
                plt.plot(
                    data["timestamp"],
                    rate / (
                        1_000_000 if unit == "seconds" else
                        60_000_000 if unit == "minutes" else
                        1_000 if unit == "ms" else 1
                    ),
                    color="red",
                )

            title = f"{resource.upper()} Pressure Rate "
            if node_count == 1:
                title += f"- {data['node'].iloc[0]}"
            elif node_count == -1:
                title += "Across Cluster"
            else:
                title += f"Across {node_count} Nodes"
            plt.title(title)
            plt.xlabel("Time")
            plt.ylabel(f"Stall time rate ({unit}/minute)")
            if node_count != 1:
                plt.legend(bbox_to_anchor=(1.05, 1))

            self._adjust_x_axis(plt.gca())
            plt.tight_layout()

            plt.savefig(
                resource_dir / f"{resource}_pressure_rate.png",
                bbox_inches="tight",
            )
            plt.close()

    def _plot_disk_stats(self, data, output_dir):
        """Plot both disk capacity and usage %."""
        disk_usages = [
            col
            for col in data.columns
            if col.startswith("disk_") and col.endswith("_usage")
        ]

        if not disk_usages:
            print("Warning: No disk usage data found to plot")
            return

        plt.title("Disk Usage")
        plt.xlabel("Time")
        plt.ylabel("% space used")

        for disk in disk_usages:
            style = self._get_process_style(len(plt.gca().lines))
            plt.plot(
                data["timestamp"],
                data[disk],
                label=disk.replace('disk_', '').replace('_usage', ''),
                **style,
            )

        self._adjust_x_axis(plt.gca())
        plt.tight_layout()
        plt.legend(bbox_to_anchor=(1.05, 1))

        plt.savefig(output_dir / f"disk_usage.png",
                    bbox_inches="tight",
                    )
        plt.close()

        disk_capacities = [
            col
            for col in data.columns
            if col.startswith("disk_") and col.endswith("_capacity")
        ]

        if not disk_capacities:
            print("Warning: No disk capacity data found to plot")
            return

        plt.title("Disk Capacity")
        plt.xlabel("Time")

        # Find appropriate unit based on max value
        max_bytes = max(data[metric].max() for metric in disk_capacities)
        _, unit = self._format_bytes(max_bytes)
        divisor = 1024 ** self.STORAGE_UNITS.index(unit)

        plt.ylabel(f"Capacity ({unit})")

        for disk in disk_capacities:
            style = self._get_process_style(len(plt.gca().lines))
            plt.plot(
                data["timestamp"],
                data[disk] / divisor,
                label=disk.replace('disk_', '').replace('_capacity', ''),
                **style,
            )

        self._adjust_x_axis(plt.gca())
        plt.tight_layout()
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.savefig(output_dir / f"disk_capacity.png",
                    bbox_inches="tight",
                    )
        plt.close()


    def _plot_cluster_top_erlang_process(self, data, output_dir, node_count):
        """Plot highest memory consuming Erlang process across cluster over
        time."""
        plt.figure(figsize=(15, 8))

        if "memory_worst_pid" in data.columns:
            valid_data = data.dropna(subset=["memory_worst_pid"])

            if len(valid_data) == 0:
                print("No valid memory consumption data found")
                return

            # For each timestamp, find row with max memory
            grouped = []
            for _, group in valid_data.groupby("timestamp"):
                max_idx = group["memory_worst_used"].idxmax()
                grouped.append({
                    "timestamp": group["timestamp"].iloc[0],
                    "memory_worst_used":
                        group.loc[max_idx, "memory_worst_used"],
                    "memory_worst_pid": group.loc[max_idx, "memory_worst_pid"],
                    "node": group.loc[max_idx, "node"]
                })
            grouped = pd.DataFrame(grouped)

            max_bytes = grouped["memory_worst_used"].max()
            _, unit = self._format_bytes(max_bytes)
            divisor = 1024 ** self.STORAGE_UNITS.index(unit)

            pid_counts = grouped["memory_worst_pid"].value_counts()
            unique_pids = sorted(
                pid_counts.index.tolist(),
                key=lambda x: pid_counts[x],
                reverse=True
            )

            for pid in unique_pids:
                pid_data = grouped[grouped["memory_worst_pid"] == pid]
                if len(pid_data) > 0:
                    node = pid_data["node"].iloc[0]
                    plt.scatter(
                        pid_data["timestamp"],
                        pid_data["memory_worst_used"] / divisor,
                        label=f"{pid} on {node} ({pid_counts[pid]} times)",
                        marker='o',
                        s=50,
                    )

            if node_count == -1:
                title = "Highest Memory Consuming Process Across Cluster"
            else:
                title = "Highest Memory Consuming Process Across "
                f"{node_count} Nodes"
            plt.title(title)
            plt.xlabel("Time")
            plt.ylabel(f"Memory Usage ({unit})")
            plt.legend(
                title="PID on Node (Frequency)",
                bbox_to_anchor=(1.05, 1)
            )
            self._adjust_x_axis(plt.gca())

            plt.tight_layout()

            plt.savefig(
                output_dir / "top_erlang_consumer.png",
                bbox_inches="tight",
            )
            plt.close()

    def _plot_cluster_system_stats(self, data, output_dir, node_count):
        """Plot key system stats across all nodes in cluster."""
        # Free memory percentage across nodes
        plt.figure(figsize=(15, 8))
        for node in self.nodes:
            node_data = data[data["node"] == node]
            if (
                "system_free_memory" in node_data.columns
                and "system_total_memory" in node_data.columns
            ):
                free_pct = (
                    node_data["system_free_memory"]
                    / node_data["system_total_memory"]
                    * 100
                )
                style = self._get_process_style(len(plt.gca().lines))
                plt.plot(node_data["timestamp"], free_pct, label=node, **style)

        title = (
            "Free Memory Percentage Across Cluster" if node_count == -1
            else f"Free Memory Percentage Across {node_count} Nodes"
        )
        plt.title(title)
        plt.xlabel("Time")
        plt.ylabel("Free Memory (%)")
        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))

        plt.tight_layout()

        memory_dir = output_dir / "memory"
        plt.savefig(memory_dir / "free_memory.png", bbox_inches="tight")
        plt.close()

        # System CPU utilization across nodes
        plt.figure(figsize=(15, 8))
        for node in self.nodes:
            node_data = data[data["node"] == node]
            if "system_cpu_utilization_rate" in node_data.columns:
                style = self._get_process_style(len(plt.gca().lines))
                plt.plot(
                    node_data["timestamp"],
                    node_data["system_cpu_utilization_rate"],
                    label=node,
                    **style,
                )

        plt.title(
            "CPU Utilization Across Cluster" if node_count == -1
            else f"CPU Utilization Across {node_count} Nodes"
        )
        plt.xlabel("Time")
        plt.ylabel("CPU Usage (%)")
        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))

        plt.tight_layout()

        cpu_dir = output_dir / "cpu"
        plt.savefig(cpu_dir / "cpu_utilization.png", bbox_inches="tight")
        plt.close()

    def _plot_cluster_allocstalls(self, data, output_dir, node_count):
        """Plot memory allocation stalls across cluster."""
        plt.figure(figsize=(15, 8))
        for node in self.nodes:
            node_data = data[data["node"] == node]
            if "system_allocstall" in node_data.columns:
                style = self._get_process_style(len(plt.gca().lines))
                plt.plot(
                    node_data["timestamp"],
                    node_data["system_allocstall"],
                    label=node,
                    **style,
                )

        plt.title(
            "Memory Allocation Stalls Across Cluster" if node_count == -1
            else f"Memory Allocation Stalls Across {node_count} Nodes"
        )
        plt.xlabel("Time")
        plt.ylabel("Allocation Stalls")
        self._adjust_x_axis(plt.gca())
        plt.legend(bbox_to_anchor=(1.05, 1))
        plt.tight_layout()

        memory_dir = output_dir / "memory"
        plt.savefig(memory_dir / "allocstalls.png", bbox_inches="tight")
        plt.close()

    def _get_metric_stats(self, data, metric, format_func=None, use_min=False):
        """Get standard stats (mean, min/max, timestamp) for a metric.

        Args:
            data: DataFrame containing the data
            metric: Column name to analyze
            format_func: Optional function to format values (e.g. _format_bytes)
            use_min: If True, track minimum value instead of maximum

        Returns:
            Dictionary with mean, min/max, and timestamp stats
        """
        valid_data = data[data[metric].notna()]
        if len(valid_data) == 0:
            return None

        mean_val = valid_data[metric].mean()
        if use_min:
            idx = valid_data[metric].idxmin()
            extreme_val = valid_data.loc[idx, metric]
        else:
            idx = valid_data[metric].idxmax()
            extreme_val = valid_data.loc[idx, metric]

        timestamp = valid_data.loc[idx, "timestamp"].strftime(
            "%Y-%m-%d %H:%M"
        )

        if format_func:
            mean_fmt, mean_unit = format_func(mean_val)
            ext_fmt, ext_unit = format_func(extreme_val)
            return {
                "mean": f"{mean_fmt:.2f} {mean_unit}",
                "min" if use_min else "max": (
                    f"{ext_fmt:.2f} {ext_unit}"
                ),
                "min_time" if use_min else "max_time": timestamp,
            }
        else:
            return {
                "mean": f"{mean_val:.2f}",
                "min" if use_min else "max": f"{extreme_val:.2f}",
                "min_time" if use_min else "max_time": timestamp,
            }

    def _get_memory_stats(self, data):
        stats = {"system": {}, "processes": {}, "erlang": {}}

        # OS Memory metrics
        for metric in ["system_total_memory", "system_total_swap"]:
            if metric in data.columns:
                metric_stats = self._get_metric_stats(
                    data, metric, self._format_bytes
                )
                if metric_stats:
                    stats["system"][metric] = metric_stats

        # Process memory stats
        for col in [c for c in data.columns if c.endswith("_mem_resident")]:
            metric_stats = self._get_metric_stats(
                data, col, self._format_bytes
            )
            if metric_stats:
                process = col.replace("_mem_resident", "")
                stats["processes"][process] = metric_stats

        if "system_allocstall" in data.columns:
            metric_stats = self._get_metric_stats(data, "system_allocstall")
            if metric_stats:
                stats["system"]["system_allocstall"] = metric_stats

        # Free memory metrics (use minimum values)
        for metric in [
            "system_free_memory",
            "system_free_swap"
        ]:
            if metric in data.columns:
                metric_stats = self._get_metric_stats(
                    data, metric, self._format_bytes, use_min=True
                )
                if metric_stats:
                    stats["system"][metric] = metric_stats

        return stats

    def _get_cpu_stats(self, data):
        """Get CPU statistics with timestamps, filtering NaN values."""
        stats = {
            "system": {},  # Overall CPU utilization
            "processes": {},  # Per-process CPU usage
        }

        # System CPU utilization
        if "system_cpu_utilization_rate" in data.columns:
            metric_stats = self._get_metric_stats(
                data, "system_cpu_utilization_rate"
            )
            if metric_stats:
                stats["system"]["utilization"] = {
                    "mean": f"{float(metric_stats['mean']):.1f}%",
                    "max": f"{float(metric_stats['max']):.1f}%",
                    "max_time": metric_stats["max_time"],
                }

        # Per-process CPU usage
        process_cols = [
            col for col in data.columns if col.endswith("_cpu_utilization")
        ]
        for col in process_cols:
            metric_stats = self._get_metric_stats(data, col)
            if metric_stats:
                process = col.replace("_cpu_utilization", "")
                stats["processes"][process] = {
                    "mean": f"{float(metric_stats['mean']):.1f}%",
                    "max": f"{float(metric_stats['max']):.1f}%",
                    "max_time": metric_stats["max_time"],
                }

        return stats

    def _get_pressure_stats(self, data):
        """Get pressure statistics with timestamps, filtering NaN values."""
        pressure_metrics = {
            "cpu": "system_cpu_pressure_some_avg60",  # CPU only has 'some'
            "memory": "system_memory_pressure_full_avg60",  # Use 'full' for mem
            "io": "system_io_pressure_full_avg60",  # Use 'full' for IO
        }

        if not any(
            metric in data.columns
            for metric in pressure_metrics.values()
        ):
            return None

        stats = {}
        for ptype, metric in pressure_metrics.items():
            if metric in data.columns:
                metric_stats = self._get_metric_stats(data, metric)
                if metric_stats:
                    stats[ptype] = {
                        "metric": metric.replace(
                            f"system_{ptype}_pressure_", ""
                        ),
                        "mean": f"{float(metric_stats['mean']):.2f}",
                        "max": f"{float(metric_stats['max']):.2f}",
                        "max_time": metric_stats["max_time"],
                    }

        return stats

    def _get_erlang_stats(self, data):
        """Get Erlang statistics with timestamps, filtering NaN values."""
        stats = {
            "memory_breakdown": {},  # From erlang:memory()
            "processes": {},  # Top memory consuming processes
        }

        # Memory breakdown categories
        memory_categories = [
            "memory_total",
            "memory_processes",
            "memory_processes_used",
            "memory_system",
            "memory_atom",
            "memory_atom_used",
            "memory_binary",
            "memory_code",
            "memory_ets",
        ]

        # Get stats for each memory category
        for category in memory_categories:
            if category in data.columns:
                metric_stats = self._get_metric_stats(
                    data, category, self._format_bytes
                )
                if metric_stats:
                    category_name = category.replace("memory_", "")
                    stats["memory_breakdown"][category_name] = metric_stats

        # Top memory consuming processes
        if "memory_worst_pid" in data.columns:
            metric_stats = self._get_metric_stats(
                data, "memory_worst_used", self._format_bytes
            )
            if metric_stats:
                max_idx = data[data["memory_worst_used"].notna()][
                    "memory_worst_used"
                ].idxmax()
                pid = data.loc[max_idx, "memory_worst_pid"]
                stats["processes"][pid] = metric_stats

        return stats

    def _write_summary(self, data, output_file):
        """Write summary statistics using raw (non-normalized) data."""
        node = data["node"].iloc[0]
        raw_node_data = self.raw_df[self.raw_df["node"] == node]

        memory_stats = self._get_memory_stats(raw_node_data)
        cpu_stats = self._get_cpu_stats(raw_node_data)
        pressure_stats = self._get_pressure_stats(raw_node_data)
        erlang_stats = self._get_erlang_stats(raw_node_data)

        with open(output_file, "w") as f:
            # Data Collection Gaps Section
            f.write("Data Collection:\n")
            f.write("---------------\n")

            node_gaps = [gap for gap in self.df.attrs["raw_collection_gaps"]
                        if gap["node"] == node]

            if node_gaps:
                f.write("\nCollection Gaps:\n")
                for gap in node_gaps:
                    duration_mins = gap['duration_mins']
                    f.write(f"  {gap['start'].strftime('%Y-%m-%d %H:%M:%S')} "
                            f"to {gap['end'].strftime('%Y-%m-%d %H:%M:%S')} "
                            f"({duration_mins:.1f} minutes)\n")
            else:
                f.write("\nNo collection gaps found\n")

            f.write("\n")

            # Memory Section
            f.write("Memory Statistics:\n")
            f.write("----------------\n")
            for metric, values in memory_stats["system"].items():
                f.write(f"\n{metric}:\n")
                f.write(f"  Mean: {values['mean']}\n")
                if "min" in values:  # For free memory metrics
                    f.write(f"  Min:  {values['min']} (at:"
                            f" {values['min_time']})\n")
                elif "max" in values:  # For total memory and allocstalls
                    f.write(f"  Max:  {values['max']} (at:"
                            f" {values['max_time']})\n")

            if "system_allocstall" in memory_stats["system"]:
                f.write("\nMemory Allocation Stalls:\n")
                f.write("----------------------\n")
                values = memory_stats["system"]["system_allocstall"]
                f.write(f"  Mean: {values['mean']}\n")
                f.write(f"  Max:  {values['max']} (at: {values['max_time']})\n")

            # Process Memory Section - sort by max memory usage in bytes
            f.write("\nProcess Memory Usage:\n")
            f.write("-------------------\n")
            sorted_processes = sorted(
                memory_stats["processes"].items(),
                key=lambda x: self._convert_to_bytes(x[1]["max"]),
                reverse=True
            )
            for process, values in sorted_processes:
                f.write(f"\n{process}:\n")
                f.write(f"  Mean: {values['mean']}\n")
                f.write(f"  Max:  {values['max']} (at: {values['max_time']})\n")

            # Erlang Memory Section - sort in logical order
            f.write("\nErlang Memory Statistics:\n")
            f.write("----------------------\n")
            f.write("\nMemory Breakdown:\n")

            # Define the desired order
            memory_order = [
                "total",          # Total memory first
                "processes",      # Process memory next
                "processes_used",
                "system",        # System memory
                "binary",        # Various memory types
                "code",
                "ets",
                "atom",
                "atom_used"
            ]

            sorted_categories = sorted(
                erlang_stats["memory_breakdown"].items(),
                key=lambda x: memory_order.index(x[0])
            )

            for category, values in sorted_categories:
                f.write(f"\n{category}:\n")
                f.write(f"  Mean: {values['mean']}\n")
                f.write(f"  Max:  {values['max']} (at: {values['max_time']})\n")

            f.write("\nTop Memory Consuming Processes:\n")
            sorted_erlang_procs = sorted(
                erlang_stats["processes"].items(),
                key=lambda x: float(x[1]["max"].split()[0]),
                reverse=True,
            )
            for pid, values in sorted_erlang_procs[:3]:  # Show top 3
                f.write(f"\n{pid}:\n")
                f.write(f"  Mean: {values['mean']}\n")
                f.write(f"  Max:  {values['max']} (at: {values['max_time']})\n")

            # CPU Section
            f.write("\nCPU Statistics:\n")
            f.write("--------------\n")
            if "utilization" in cpu_stats["system"]:
                values = cpu_stats["system"]["utilization"]
                f.write("\nSystem CPU Usage:\n")
                f.write(f"  Mean: {values['mean']}\n")
                f.write(f"  Max:  {values['max']} (at: {values['max_time']})\n")

            if pressure_stats:
                f.write("\nPressure Statistics:\n")
                f.write("-------------------\n")
                for ptype, values in pressure_stats.items():
                    f.write(f"\n{ptype.upper()} ({values['metric']}):\n")
                    f.write(f"  Mean: {values['mean']}%\n")
                    f.write(f"  Max:  {values['max']}% (at: "
                            f"{values['max_time']})\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze stats log")
    parser.add_argument(
        "-l",
        "--log",
        dest="log_file",
        help="path to stats log file",
        default="logs/n_0/stats.log",
    )
    parser.add_argument(
        "--nodes", dest="nodes", help="comma-separated list of nodes to analyze"
    )
    parser.add_argument(
        "-s",
        "--start",
        dest="ts_start",
        help='start time to filter (e.g. "2023-06-20T18:00")',
    )
    parser.add_argument(
        "-e",
        "--end",
        dest="ts_end",
        help='end time to filter (e.g. "2023-06-20T18:30")',
    )
    parser.add_argument(
        "-o",
        "--output",
        dest="output_dir",
        help="output directory",
        default="analysis",
    )

    args = parser.parse_args()

    analyzer = StatsAnalyzer(args.log_file, args.ts_start, args.ts_end)
    if analyzer.set_nodes(args.nodes):
        analyzer.generate_analysis(args.output_dir)
