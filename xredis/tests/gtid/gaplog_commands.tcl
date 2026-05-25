# Gaplog 命令单测
#
# 测试所有 GTIDX GAPLOG 子命令的输出格式
#
# 命令列表:
#   GTIDX GAPLOG LEN          -> integer (条目数量)
#   GTIDX GAPLOG RANGE <uuid> <start> <end> -> array [gno, keys_infos, gno, keys_infos, ...]
#   GTIDX GAPLOG CLEAR        -> OK
#
# 注意: GAPLOG ADD 命令已删除，gaplog 通过 saveGapLogFromGtidSet 自动填充

# =====================================================
# GAPLOG LEN 命令测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-001: GAPLOG LEN - empty gaplog returns 0" {
        # 空的 gaplog 应该返回 0
        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }

    test "GAPLOG-CMD-002: GAPLOG CLEAR - clears all entries" {
        # CLEAR 命令应该返回 OK
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"

        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }
}

# =====================================================
# GAPLOG RANGE 命令测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-003: GAPLOG RANGE - empty gaplog returns empty array" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "uuid-001" 1 10]
        # 空数组
        assert_equal $result {}
    }

    test "GAPLOG-CMD-004: GAPLOG RANGE - non-existent uuid returns empty array" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "non-existent-uuid" 1 10]
        assert_equal $result {}
    }

    test "GAPLOG-CMD-005: GAPLOG RANGE - start > end returns empty" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "uuid-001" 5 1]
        assert_equal $result {}
    }

    test "GAPLOG-CMD-006: GAPLOG RANGE - start beyond max gno returns empty" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG RANGE "uuid-001" 10 20]
        assert_equal $result {}
    }
}

# =====================================================
# GAPLOG CLEAR 命令测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-007: GAPLOG CLEAR - empty gaplog returns OK" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"
        set len [r GTIDX GAPLOG LEN]
        assert {$len == 0}
    }

    test "GAPLOG-CMD-008: GAPLOG CLEAR - RANGE returns empty after clear" {
        r GTIDX GAPLOG CLEAR
        set result [r GTIDX GAPLOG CLEAR]
        assert_equal $result "OK"

        set result [r GTIDX GAPLOG RANGE "uuid-001" 1 10]
        assert_equal $result {}
    }
}

# =====================================================
# 错误处理测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-009: GAPLOG LEN - wrong args count error" {
        catch {r GTIDX GAPLOG LEN "extra-arg"} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-CMD-010: GAPLOG RANGE - wrong args count error" {
        catch {r GTIDX GAPLOG RANGE "uuid"} err
        assert_match "*wrong*" $err
    }

    test "GAPLOG-CMD-011: GAPLOG RANGE - invalid gno format error" {
        catch {r GTIDX GAPLOG RANGE "uuid-001" "abc" "def"} err
        assert_match "*integer*" $err
    }

    test "GAPLOG-CMD-012: GAPLOG invalid subcommand error" {
        catch {r GTIDX GAPLOG INVALID} err
        assert_match "*subcommand*" $err
    }
}

# =====================================================
# HELP 命令测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-013: GTIDX HELP contains GAPLOG commands" {
        set help [r GTIDX HELP]
        assert_match "*GAPLOG LEN*" $help
        assert_match "*GAPLOG RANGE*" $help
        assert_match "*GAPLOG CLEAR*" $help
    }
}

# =====================================================
# INFO 统计测试
# =====================================================
start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
    test "GAPLOG-CMD-014: INFO GTID contains gaplog stats" {
        r GTIDX GAPLOG CLEAR
        set info [r INFO gtid]
        # 验证 gaplog 统计信息
        assert_match "*gtid_gaplog_entries:0*" $info
    }
}

start_server {tags {"gaplog"} overrides {gtid-enabled yes gtid-gaplog-enabled yes}} { 
    start_server {overrides {gtid-enabled yes gtid-gaplog-enabled yes}} {
        set M [srv -1 client]
        set M_host [srv -1 host]
        set M_port [srv -1 port]
        set S [srv 0 client]


        test "MULTI/EXEC transaction should record to gaplog" {
            # 1. Slave 同步 Master
            $S replicaof $M_host $M_port
            wait_for_sync $S

            # 2. Master 写入数据
            $M set m_key m_val
            wait_for_sync $S

            # 3. Slave 断开并独立写入 MULTI/EXEC
            $S replicaof no one
            after 100

            # Slave 独立写入 MULTI/EXEC 事务
            $S MULTI
            $S set s_key1 s_val1
            $S set s_key2 s_val2
            $S hset s_hash field1 val1 field2 val2
            $S EXEC

            # 4. Slave 重新连接
            $S replicaof $M_host $M_port
            wait_for_sync $S

            after 500

            # 5. 验证 gaplog
            set info [$S INFO gtid]
            puts "\nSlave GTID Info:"
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_*" $line]} {
                    puts $line
                }
            }

            # 验证数据
            assert_equal [$S get m_key] m_val
            assert_equal [$S get s_key1] s_val1
            assert_equal [$S get s_key2] s_val2
            assert_equal [$S hget s_hash field1] val1
            assert_equal [$S hget s_hash field2] val2
        }
       
   
        test "GAPLOG LIST command - list entries in global order" {
            # 获取 slave 的 uuid
            set info [$S INFO gtid]
            set uuid ""
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_uuid:*" $line]} {
                    set uuid [string range $line 10 end]
                    break
                }
            }
            assert {$uuid != ""}

            # 获取 gaplog 长度
            set gaplog_len [$S GTIDX GAPLOG LEN]
            assert {$gaplog_len > 0}
            puts "gaplog len: $gaplog_len"

            # 测试 LIST 命令返回所有条目
            set list_result [$S GTIDX GAPLOG LIST 0 $gaplog_len]
            assert {[llength $list_result] > 0}
            puts "list_result len: [llength $list_result]"

            # 验证 LIST 返回格式: [uuid, gno, [keyinfos...]]
            set first_entry [lindex $list_result 0]
            assert {[llength $first_entry] == 3}
            set entry_uuid [lindex $first_entry 0]
            set entry_gno [lindex $first_entry 1]
            assert_equal $entry_uuid $uuid
            assert {$entry_gno > 0}

            # 测试 LIST 分页: 只返回1条
            set list_result2 [$S GTIDX GAPLOG LIST 0 1]
            assert {[llength $list_result2] == 1}

            # 测试 LIST 从中间位置开始
            if {$gaplog_len >= 2} {
                set list_result3 [$S GTIDX GAPLOG LIST 1 1]
                assert {[llength $list_result3] == 1}
                set second_entry [lindex $list_result 1]
                set third_entry [lindex $list_result3 0]
                assert_equal [lindex $second_entry 1] [lindex $third_entry 1]
            }
        }

        test "GAPLOG DELETERANGE command - delete single entry" {
            # 获取 slave 的 uuid
            set info [$S INFO gtid]
            set uuid ""
            foreach line [split $info "\r\n"] {
                if {[string match "gtid_uuid:*" $line]} {
                    set uuid [string range $line 10 end]
                    break
                }
            }
            assert {$uuid != ""}

            set gaplog_len [$S GTIDX GAPLOG LEN]
            assert {$gaplog_len > 0}

            # 先获取第一个 gno
            set list_result [$S GTIDX GAPLOG LIST 0 1]
            set first_entry [lindex $list_result 0]
            set first_gno [lindex $first_entry 1]
            puts "first_gno: $first_gno"

            # 测试 DELETERANGE 删除单个条目
            set deleted [$S GTIDX GAPLOG DELETERANGE $uuid $first_gno $first_gno]
            assert {$deleted > 0}
            puts "deleted: $deleted"

            # 验证删除后长度减少
            set new_len [$S GTIDX GAPLOG LEN]
            assert {$new_len == [expr {$gaplog_len - $deleted}]}
        }
    }
}