
proc get_info_property {r section line property} {
    set str [$r info $section]
    if {[regexp ".*${line}:\[^\r\n\]*${property}=(\[^,\r\n\]*).*" $str match submatch]} {
        set _ $submatch
    }
}

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
            wait_for_sync $S

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
            assert_match "*s_key1*" $result
            assert_match "*s_key2*" $result


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

                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                $MA set ma_key1 ma_val1
                $MA set ma_key2 ma_val2
                $MA set ma_key3 ma_val3
                wait_for_sync $S

                $S replicaof no one
                after 100

                $MA set ma_key4 ma_val4
                $MA set ma_key5 ma_val5

                set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

                $S replicaof $MA_host $MA_port
                wait_for_sync $S

                wait_for_condition 50 100 {
                    [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
                } else {
                    fail "xsync xcontinue not detected"
                }

                set gaplog_len [get_gaplog_entries $S]
                assert_equal $gaplog_len 0

                assert_equal [$S get ma_key1] ma_val1
                assert_equal [$S get ma_key4] ma_val4
                assert_equal [$S get ma_key5] ma_val5
            }
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-003: slave independent MULTI/EXEC transaction" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_sync $S

            
            $S replicaof no one
            after 100

            $S select 1
            $S MULTI
            $S set s_multi_key1 s_multi_val1
            $S select 2
            $S set s_multi_key2 s_multi_val2
            $S select 3
            $S set s_multi_key3 s_multi_val3
            $S EXEC

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1


            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]

            assert_match "*s_multi_key1*" $result
            assert_match "*s_multi_key2*" $result
            assert_match "*s_multi_key3*" $result

            $S select $::target_db
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

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-004: slave independent Lua script multi-key" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_sync $S

            $S replicaof no one
            after 100

            set lua_script {
                redis.call("SET", KEYS[1], ARGV[1])
                redis.call("SET", KEYS[2], ARGV[2])
                redis.call("SET", KEYS[3], ARGV[3])
                return "OK"
            }
            $S EVAL $lua_script 3 s_lua_key1 s_lua_key2 s_lua_key3 s_lua_val1 s_lua_val2 s_lua_val3

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            assert_match "*s_lua_key1*" $result
            assert_match "*s_lua_key2*" $result
            assert_match "*s_lua_key3*" $result

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_lua_key1] s_lua_val1
            assert_equal [$S get s_lua_key2] s_lua_val2
            assert_equal [$S get s_lua_key3] s_lua_val3
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    test "GAPLOG-XSYNC-005: config and basic commands" {
        set client [srv 0 client]

        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled yes} $enabled

        $client CONFIG SET gtid-gaplog-enabled no
        set enabled [$client CONFIG GET gtid-gaplog-enabled]
        assert_equal {gtid-gaplog-enabled no} $enabled

        $client CONFIG SET gtid-gaplog-enabled yes

        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 10000} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 100
        set max_gap [$client CONFIG GET gtid-xsync-max-gap]
        assert_equal {gtid-xsync-max-gap 100} $max_gap

        $client CONFIG SET gtid-xsync-max-gap 10000

        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0

        $client GTIDX GAPLOG CLEAR
        set len [$client GTIDX GAPLOG LEN]
        assert_equal $len 0
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-006: edge case - empty data sync" {

            $S replicaof $M_host $M_port
            wait_for_sync $S

            $S replicaof no one
            after 100

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 0
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-007: edge case - many independent writes" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_sync $S

            $S replicaof no one
            after 100

            set num_writes 10
            for {set i 1} {$i <= $num_writes} {incr i} {
                $S set s_key_$i s_val_$i
            }

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len $num_writes

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 $num_writes]
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_match "*s_key_$i*" $result
            }

            assert_equal [$S get m_key] m_val
            for {set i 1} {$i <= $num_writes} {incr i} {
                assert_equal [$S get s_key_$i] s_val_$i
            }
        }
    }
}


start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-008: edge case - different data types" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key m_val
            wait_for_sync $S

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

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 5

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 5]
            assert_match "*s_str_key*" $result
            assert_match "*s_hash_key*" $result
            assert_match "*s_list_key*" $result
            assert_match "*s_set_key*" $result
            assert_match "*s_zset_key*" $result

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_str_key] s_str_val
            assert_equal [$S hget s_hash_key field1] val1
            assert_equal [$S lrange s_list_key 0 -1] {elem3 elem2 elem1}
            assert_equal [lsort [$S smembers s_set_key]] {member1 member2}
            assert_equal [$S zrange s_zset_key 0 -1] {member1 member2}
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-009: edge case - SELECT command" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            $M set m_key m_val
            wait_for_sync $S

            assert_equal [$S get m_key] m_val

            $S replicaof no one
            after 100

            $S set s_db0_key s_db0_val

            set slave_uuid [get_slave_gtid_uuid $S]

            set orig_xcontinue [get_info_property $S gtid gtid_sync_stat xsync_xcontinue]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            wait_for_condition 50 100 {
                [get_info_property $S gtid gtid_sync_stat xsync_xcontinue] > $orig_xcontinue
            } else {
                fail "xsync xcontinue not detected"
            }

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 1

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 1]
            assert_match "*s_db0_key*" $result

            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_db0_key] s_db0_val
        }
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes gtid-xsync-max-gap 10000}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]

        test "GAPLOG-XSYNC-010: edge case - DEL command" {
            
            $S replicaof $M_host $M_port
            wait_for_sync $S

            
            $M set m_key1 m_val1
            $M set m_key2 m_val2
            wait_for_sync $S

            $S replicaof no one
            after 100

            $S set s_key s_val
            $S del m_key1

            set slave_uuid [get_slave_gtid_uuid $S]

            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 100

            set gaplog_len [get_gaplog_entries $S]
            assert_equal $gaplog_len 2

            set result [$S GTIDX GAPLOG RANGE $slave_uuid 1 2]
            assert_match "*s_key*" $result
            assert_match "*m_key1*" $result

            assert_equal [$S get m_key2] m_val2
            assert_equal [$S get s_key] s_val
        }
    }
}