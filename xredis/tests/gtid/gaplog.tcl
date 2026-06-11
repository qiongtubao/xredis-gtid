

proc get_gaplog_entries {client} {
    set info [$client INFO gtid]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_gaplog_entries:*" $line]} {
            return [string range $line 20 end]
        }
    }
    return 0
}

proc get_uuid {client} {
    set info [$client INFO server]
    foreach line [split $info "\r\n"] {
        if {[string match "gtid_uuid:*" $line]} {
            return [string range $line 11 end]
        }
    }
    return ""
}

proc get_slave_gtid_uuid {client} {
    set seq [$client GTIDX seq gtid.set]
    set parts [split $seq ","]
    if {[llength $parts] >= 2} {
        set uuid_gno [lindex $parts 1]
        set uuid [lindex [split $uuid_gno ":"] 0]
        return $uuid
    } elseif {[llength $parts] == 1} {
        set uuid_gno [lindex $parts 0]
        set uuid [lindex [split $uuid_gno ":"] 0]
        return $uuid
    }
    return ""
}

proc parse_gaplog_keys {gaplog_result} {
    set keys {}
    set lines [split $gaplog_result "\r\n"]
    foreach line $lines {
        if {[string match "*:*" $line] || $line eq ""} {
            continue
        }
        if {[string match "s_*" $line] || [string match "m_*" $line]} {
            lappend keys $line
        }
    }
    return $keys
}

proc gaplog_contains_key {client uuid gno_start gno_end expected_keys} {
    catch {
        set result [$client GTIDX GAPLOG RANGE $uuid $gno_start $gno_end]
    }

    set found_keys {}

    foreach key $expected_keys {
        if {[string match "*$key*" $result]} {
            lappend found_keys $key
        }
    }

    return [list [llength $found_keys] $found_keys $result]
}

# =====================================================
# GAPLOG-XSYNC-001: Correct xcontinue trigger scenario
# Slave reconnects to same Master after disconnection
# Verify gaplog recorded keys are correct
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-001: slave reconnects same master triggers xcontinue" {
            $S replicaof $M_host $M_port
            wait_for_sync $S

            $M set m_key1 m_val1
            $M set m_key2 m_val2
            wait_for_ofs_sync $S $M

            $S replicaof no one
            after 100

            $S set s_key1 s_val1
            $S set s_key2 s_val2

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 2


            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 2]

            set result_str [join $result " "]
            assert_match "*s_key1*" $result_str
            assert_match "*s_key2*" $result_str

            assert_equal [$S get m_key1] m_val1
            assert_equal [$S get m_key2] m_val2

            assert_equal [$S get s_key1] s_val1
            assert_equal [$S get s_key2] s_val2
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
            set MA [srv -2 client]
            set MA_host [srv -2 host]
            set MA_port [srv -2 port]
            set MB [srv -1 client]
            set MB_host [srv -1 host]
            set MB_port [srv -1 port]
            set S [srv 0 client]

            test "GAPLOG-XSYNC-002: master's GTID in gaplog after slave switch" {
                # 1. Slave sync MasterA
                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                # 2. MasterA writes data (GTIDs recorded in slave's gtid_seq)
                $MA set ma_key1 ma_val1
                $MA set ma_key2 ma_val2
                $MA set ma_key3 ma_val3
                wait_for_ofs_sync $S $MA

                # 3. Slave disconnects
                $S replicaof no one
                after 100

                # 4. MasterA continues writing data
                $MA set ma_key4 ma_val4
                $MA set ma_key5 ma_val5

                # 5. Slave reconnects to MasterA
                set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                wait_for_condition 50 100 {
                    [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
                } else {
                    fail "xsync xcontinue not detected"
                }

                # 6. Verify gaplog is empty
                set gaplog_len [get_gaplog_entries $S]
                assert_equal $gaplog_len 0

                # Verify data consistency
                assert_equal [$S get ma_key1] ma_val1
                assert_equal [$S get ma_key4] ma_val4
                assert_equal [$S get ma_key5] ma_val5
            }
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-003: Slave independent MULTI/EXEC transaction test
# MULTI/EXEC transaction generates one GTID containing all keys
# Verify gaplog keys include all keys within the transaction
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-003: slave independent MULTI/EXEC transaction" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            # 3. Slave disconnects and writes MULTI/EXEC independently
            $S replicaof no one
            after 100

            # Slave writes MULTI/EXEC transaction independently
            $S MULTI
            $S set s_multi_key1 s_multi_val1
            $S select 2
            $S set s_multi_key2 s_multi_val2
            $S select 3
            $S set s_multi_key3 s_multi_val3
            $S EXEC

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            # Wait for xsync continue
            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            # 5. Verify gaplog entry count: MULTI/EXEC generates 1 GTID
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            # 6. Verify gaplog keys include all keys within the transaction
            # Query gaplog range, verify all 3 keys are present
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            puts $result
            # Result should contain s_multi_key1, s_multi_key2, s_multi_key3
            assert_match "*s_multi_key1*" $result
            assert_match "*s_multi_key2*" $result
            assert_match "*s_multi_key3*" $result

            # Verify data
            assert_equal [$S get m_key] m_val
            $S select 1
            assert_equal [$S get s_multi_key1] s_multi_val1
            $S select 2
            assert_equal [$S get s_multi_key2] s_multi_val2
            $S select 3
            assert_equal [$S get s_multi_key3] s_multi_val3
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-004: Slave independent Lua script test
# Lua script generates one GTID containing all keys
# Verify gaplog keys include all keys within the script
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-004: slave independent Lua script multi-key" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            # 3. Slave disconnects and writes Lua script independently
            $S replicaof no one
            after 100

            # Slave writes Lua script independently
            set lua_script {
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("SET", KEYS[2], ARGV[2])
                redis.call("SET", KEYS[3], ARGV[3])
                return "OK"
            }
            $S EVAL $lua_script 3 s_lua_key1 s_lua_key2 s_lua_key3 s_lua_val1 s_lua_val2 s_lua_val3

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            # Wait for xsync continue
            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            # 5. Verify gaplog entry count: Lua script generates 1 GTID
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            # 6. Verify gaplog keys include all keys within the script
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            # Result should contain s_lua_key1, s_lua_key2, s_lua_key3
            assert_match "*s_lua_key1*" $result
            assert_match "*s_lua_key2*" $result
            assert_match "*s_lua_key3*" $result

            # Verify data
            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_lua_key1] s_lua_val1
            assert_equal [$S get s_lua_key2] s_lua_val2
            assert_equal [$S get s_lua_key3] s_lua_val3
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-005: Config and basic commands test
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    test "GAPLOG-XSYNC-005: config and basic commands" {
        set client [srv 0 client]

        # Test config
        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled yes} $enabled

        $client CONFIG SET gtid-gaplog-enabled no
        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled no} $enabled

        $client CONFIG SET gtid-gaplog-enabled yes

        # Test gtid-xsync-max-gap (FIFO eviction threshold)
        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 10000} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 100
        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 100} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 10000

        # Test GAPLOG LEN
        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0

        # Test GAPLOG CLEAR
        $client GTIDX GAPLOG CLEAR
        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0
    }
}

# =====================================================
# GAPLOG-XSYNC-006: Edge case - empty data sync
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-006: edge case - empty data sync" {
            # 1. Slave sync Master (no data)
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Slave disconnects (no independent writes)
            $S replicaof no one
            after 100

            # 3. Slave reconnects
            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            # 4. Verify gaplog is empty
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 0
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-007: Edge case - many independent writes
# Verify gaplog recorded key count is correct
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-007: edge case - many independent writes" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            # 3. Slave disconnects and writes many keys independently
            $S replicaof no one
            after 100

            set num_writes 10
            for {set i 1} {$i <= $num_writes} {incr i} {
                $S set s_key_$i s_val_$i
            }

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            # 5. Verify gaplog entry count
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len $num_writes

            # 6. Verify gaplog recorded keys are correct
            # Query gaplog range, verify all keys are present
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $num_writes]
            # Verify all keys are in the result
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_match "*s_key_$i*" $result_str
            }

            # Verify data
            assert_equal [$S get m_key] m_val
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_equal [$S get s_key_$i] s_val_$i
            }
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-008: Edge case - different data types
# Verify gaplog keys for different data types are correct
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-008: edge case - different data types" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            # 3. Slave disconnects and writes different data types
            $S replicaof no one
            after 100

            # String
            $S set s_str_key s_str_val
            # Hash
            $S hset s_hash_key field1 val1 field2 val2
            # List
            $S lpush s_list_key elem1 elem2 elem3
            # Set
            $S sadd s_set_key member1 member2
            # Sorted Set
            $S zadd s_zset_key 1 member1 2 member2

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            # 5. Verify gaplog entry count(5 data types)
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 5

            # 6. Verify gaplog recorded keys are correct
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 5]
            # Verify all keys are in the result
            assert_match "*s_str_key*" $result
            assert_match "*s_hash_key*" $result
            assert_match "*s_list_key*" $result
            assert_match "*s_set_key*" $result
            assert_match "*s_zset_key*" $result

            # Verify data
            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_str_key] s_str_val
            assert_equal [$S hget s_hash_key field1] val1
            assert_equal [$S lrange s_list_key 0 -1] {elem3 elem2 elem1}
            assert_equal [lsort [$S smembers s_set_key]] {member1 member2}
            assert_equal [$S zrange s_zset_key 0 -1] {member1 member2}
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-009: Edge case - SELECT command
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-009: edge case - SELECT command" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data to db0
            $M set m_key m_val
            wait_for_ofs_sync $S $M

            # Verify slave has data in db0
            assert_equal [$S get m_key] m_val

            # 3. Slave disconnects and writes to db0 (no db switch)
            $S replicaof no one
            after 100

            # Write data to db0
            $S set s_db0_key s_db0_val

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            # Wait for xsync continue
            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            # 5. Verify gaplog entry count(1 write command)
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            # 6. Verify gaplog recorded keys are correct
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            set result_str [join $result " "]
            assert_match "*s_db0_key*" $result_str

            # Verify data (slave currently in db0)
            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_db0_key] s_db0_val
        }
    }
}

# =====================================================
# GAPLOG-XSYNC-010: Edge case - DEL command
# Verify gaplog keys for DEL command are correct
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-010: edge case - DEL command" {
            # 1. Slave sync Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master writes data
            $M set m_key1 m_val1
            $M set m_key2 m_val2
            wait_for_ofs_sync $S $M

            # 3. Slave disconnects and writes DEL command
            $S replicaof no one
            after 100

            # Slave independent write
            $S set s_key s_val
            # Slave deletes master key
            $S del m_key1

            # Get slave independent write GTID uuid
            set slave_uuid [get_slave_gtid_uuid $S]

            # 4. Slave reconnects
            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            # 5. Verify gaplog entry count(2 commands: SET and DEL)
            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 2

            # 6. Verify gaplog recorded keys are correct
            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 2]
            # Verify s_key and m_key1 are in results
            assert_match "*s_key*" $result
            assert_match "*m_key1*" $result

            # Verify data
            assert_equal [$S get m_key2] m_val2
            assert_equal [$S get s_key] s_val
        }
    }
}
