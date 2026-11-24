CREATE SCHEMA IF NOT EXISTS tpch;
SET search_path = tpch;

CREATE FOREIGN TABLE tpch.customer (
    c_custkey bigint,
    c_name text,
    c_address text,
    c_nationkey integer,
    c_phone text,
    c_acctbal numeric(15,2),
    c_mktsegment text,
    c_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.customer');

CREATE FOREIGN TABLE tpch.lineitem (
    l_orderkey bigint,
    l_partkey bigint,
    l_suppkey bigint,
    l_linenumber bigint,
    l_quantity numeric(15,2),
    l_extendedprice numeric(15,2),
    l_discount numeric(15,2),
    l_tax numeric(15,2),
    l_returnflag text,
    l_linestatus text,
    l_shipdate date,
    l_commitdate date,
    l_receiptdate date,
    l_shipinstruct text,
    l_shipmode text,
    l_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.lineitem');

CREATE FOREIGN TABLE tpch.nation (
    n_nationkey integer,
    n_name text,
    n_regionkey integer,
    n_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.nation');

CREATE FOREIGN TABLE tpch.orders (
    o_orderkey bigint,
    o_custkey bigint,
    o_orderstatus text,
    o_totalprice numeric(15,2),
    o_orderdate date,
    o_orderpriority text,
    o_clerk text,
    o_shippriority integer,
    o_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.orders');

CREATE FOREIGN TABLE tpch.part (
    p_partkey bigint,
    p_name text,
    p_mfgr text,
    p_brand text,
    p_type text,
    p_size integer,
    p_container text,
    p_retailprice numeric(15,2),
    p_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.part');

CREATE FOREIGN TABLE tpch.partsupp (
    ps_partkey bigint,
    ps_suppkey bigint,
    ps_availqty bigint,
    ps_supplycost numeric(15,2),
    ps_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.partsupp');

CREATE FOREIGN TABLE tpch.region (
    r_regionkey integer,
    r_name text,
    r_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.region');

CREATE FOREIGN TABLE tpch.supplier (
    s_suppkey bigint,
    s_name text,
    s_address text,
    s_nationkey integer,
    s_phone text,
    s_acctbal numeric(15,2),
    s_comment text
) SERVER smoke_server
    OPTIONS (table 'tpch.supplier');
